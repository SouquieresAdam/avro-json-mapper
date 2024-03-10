package com.michelin.avroxmlmapper.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.michelin.avroxmlmapper.exception.AvroJsonMapperException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;

import static com.michelin.avroxmlmapper.utility.GenericUtils.extractRealType;
import static com.michelin.avroxmlmapper.utility.GenericUtils.parseValue;

/**
 * Utility class for converting Json to Avro.
 */
public final class JsonToAvroUtils {

    /**
     * Converts, recursively, the content of an JSON-node into SpecificRecord (avro).
     *
     * @param currentNode         JSON-node to convert
     * @param clazz            class of the SpecificRecord to generate
     * @param baseNamespace    base namespace for the generated SpecificRecord classes
     * @param jsonpathSelector    the jsonpathSelector property used to search for the jsonpath mapping in the Avro definition
     * @param <T>              The type of the Avro object
     * @return SpecificRecord generated
     */
    static <T extends SpecificRecordBase> T convert(JsonNode currentNode, String baseNamespace, Class<T> clazz, String jsonpathSelector) {
        try {
            T record = clazz.getDeclaredConstructor().newInstance();
            for (Schema.Field field : record.getSchema().getFields()) {
                Schema fieldType = extractRealType(field.schema());
                var fieldNode = resolveNode(currentNode, field.getProp(jsonpathSelector));

                if(fieldNode == null) {
                   continue;
                }

                switch (fieldType.getType()) {
                    case NULL:
                    case UNION:
                    case ENUM:
                        // nothing
                        break;
                    case RECORD:
                        record.put(field.name(), convert(fieldNode, baseNamespace, baseClass(baseNamespace, fieldType.getName()), jsonpathSelector));
                        break;
                    case ARRAY:
                        var jsonEntries = fieldNode.elements();
                        var avroArray = new ArrayList<>();
                        record.put(field.name(), avroArray);

                        var elementSchema = fieldType.getElementType();
                        if (extractRealType(elementSchema).getType() == Schema.Type.RECORD) { // an array of records
                            jsonEntries.forEachRemaining(childJsonNode -> {
                                try {
                                    avroArray.add(convert(childJsonNode, baseNamespace, baseClass(baseNamespace, fieldType.getElementType().getName()), jsonpathSelector));
                                } catch (ClassNotFoundException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                        } else {
                            // Primitive types
                            jsonEntries.forEachRemaining(childJsonNode -> {
                                // for arrays, we use the path selector on the child type to match the array value
                                var stringValue = resolveNode(childJsonNode, fieldType.getProp(jsonpathSelector)).textValue();
                                    avroArray.add(parseValue(extractRealType(elementSchema).getType(), stringValue));
                            });
                        }
                        break;
                    case MAP:
                        break;
                    case LONG:
                        var longFieldValue = fieldNode.textValue();

                        //Handle dates to a TimezonedTimestamp format
                        if (fieldType.getLogicalType() != null && fieldType.getLogicalType().getName().equals("timestamp-millis")) {
                            record.put(field.name(), convertJsonDateToAvro(longFieldValue, field, fieldType));
                        } else {
                            record.put(field.name(), Long.parseLong(longFieldValue));
                        }
                        break;
                    case BYTES:
                        break;
                    default:
                        var stringFieldValue = fieldNode.textValue();
                        record.put(field.name(), parseValue(fieldType.getType(), stringFieldValue));
                }
            }
            return record;
        } catch (Exception e) {
            throw new AvroJsonMapperException("Failed to parse document", e);
        }
    }

    // If the path parameter contains dot or array notation, we need to resolve it
    // Returns the resolved node
    private static JsonNode resolveNode(JsonNode node, String path) {

        if (path == null) {
            return null;
        }
        var subPaths = path.split("\\.");

        var effectiveNode = node;

        for(var subPath : subPaths) {
            if(subPath.contains("[")) {
                var arrayPath = subPath.split("\\[");
                var arrayIndex = Integer.parseInt(arrayPath[1].replace("]", ""));
                effectiveNode = effectiveNode.get(arrayPath[0]).get(arrayIndex);
            } else {
                effectiveNode = effectiveNode.get(subPath);
            }
        }

        return effectiveNode;
    }


    private static Instant convertJsonDateToAvro(String dateString, Schema.Field field, Schema fieldType) {

        Instant resultDate = null;

            if (dateString != null && !dateString.isEmpty()) {
                //convert to date
                resultDate = convertUnknownFormatDateToTimestamp(dateString);
            }

        if (resultDate == null && field.hasDefaultValue() && field.defaultVal() != JsonProperties.NULL_VALUE) {
            resultDate = Instant.ofEpochMilli((Long) field.defaultVal());

        }
        return resultDate;
    }


    /**
     * Tries to convert the string date using a number of known patterns. Throws a DateTimeParseException if nothing worked
     *
     * @param date The string date to convert
     * @return The timestamp corresponding to the initial string
     */
    private static Instant convertUnknownFormatDateToTimestamp(String date) throws DateTimeParseException {
        try {
            return convertISO8601DateTimeToTimestamp(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return convertISO8601DateToTimestamp(date);
        } catch (DateTimeException ignored) {
        }
        try {
            return convertFlatDateToTimestamp(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return convertFlatDateTimeToTimestamp(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return convertISO8601DateTimeNoOffsetToTimestamp(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return convertISO8601DateNoOffsetToTimestamp(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return convertFlatDateNoOffsetToTimestamp(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return convertFlatDateTimeNoOffsetToTimestamp(date);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return convertFlatDateTimeNoOffsetWithoutZoneToTimestamp(date);
        } catch (ParseException ignored) {
        }
        try {
            return convertFlatDateTimeWithOffsetZoneToTimestamp(date);
        } catch (ParseException ignored) {
        }
        return null;
    }

    private static Instant convertISO8601DateTimeToTimestamp(String s) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(s, DateTimeFormatter.ISO_DATE_TIME);

        return zonedDateTime.toInstant();
    }

    private static Instant convertISO8601DateTimeNoOffsetToTimestamp(String s) {
        return convertISO8601DateTimeToTimestamp(s + "Z");
    }

    private static Instant convertISO8601DateToTimestamp(String s) {

        TemporalAccessor parsed = DateTimeFormatter.ISO_DATE.parse(s);
        ZoneId zone = ZoneId.from(parsed);
        String noonFormattedDate = s.replace(zone.getId(), "T12:00Z");
        return convertISO8601DateTimeToTimestamp(noonFormattedDate);
    }

    private static Instant convertISO8601DateNoOffsetToTimestamp(String s) {
        return convertISO8601DateTimeToTimestamp(s + "T00:00Z");
    }

    private static Instant convertFlatDateToTimestamp(String s) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddz");
        TemporalAccessor parsed = formatter.parse(s);
        ZoneId zone = ZoneId.from(parsed);
        String noonFormattedDate = s.replace(zone.getId(), "120000Z");

        return convertFlatDateTimeToTimestamp(noonFormattedDate);
    }

    private static Instant convertFlatDateTimeToTimestamp(String s) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssz");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(s, formatter);
        return zonedDateTime.toInstant();
    }

    private static Instant convertFlatDateNoOffsetToTimestamp(String s) {
        return convertFlatDateTimeToTimestamp(s + "120000Z");
    }

    private static Instant convertFlatDateTimeNoOffsetToTimestamp(String s) {
        return convertFlatDateTimeToTimestamp(s + "Z");
    }

    private static Instant convertFlatDateTimeNoOffsetWithoutZoneToTimestamp(String s) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        var result = formatter.parse(s);

        return result.toInstant();
    }

    private static Instant convertFlatDateTimeWithOffsetZoneToTimestamp(String s) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'T'00:00");
        var result = formatter.parse(s);
        return result.toInstant();
    }


    @SuppressWarnings("unchecked")
    private static Class<SpecificRecordBase> baseClass(String baseNamespace, String typeName) throws ClassNotFoundException {
        return (Class<SpecificRecordBase>) Class.forName(baseNamespace + "." + typeName);
    }
}
