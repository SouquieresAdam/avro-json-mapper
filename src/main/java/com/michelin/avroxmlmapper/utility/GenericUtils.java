package com.michelin.avroxmlmapper.utility;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.michelin.avroxmlmapper.exception.AvroJsonMapperException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Generic utility class for conversions.
 */
public final class GenericUtils {

    // Logger instance
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericUtils.class);


    /**
     * Converts a json document to a String.
     *
     * @param document the document to convert
     * @return the result string
     */
    public static String jsonnodeToString(JsonNode document) {
        return document.toString();
    }

    /**
     * Evaluate a string value as a jasckson JsonNode
     *
     * @param strValue         the string value to evaluate as a json Document
     * @return the evaluated json Document
     */
    public static ObjectNode stringToDocument(String strValue) {

        try {
            ObjectMapper mapper = new ObjectMapper();
            return (ObjectNode) mapper.readTree(strValue);
        } catch (JsonProcessingException e) {
            LOGGER.error("Json Message not parsable", e);
            return null;
        }
    }

    /**
     *
     * @param node             the JsonNode to evaluate
     * @param jsonPathExpression  the jsonpath expression to match
     * @return the list of matched nodes
     */
    public static List<JsonNode> jsonpathNodeListEvaluation(JsonNode node, String jsonPathExpression) {
        List<JsonNode> result = null;

        try {
            result = node.findValues(jsonPathExpression);
        } catch (Exception e) {
            throw new AvroJsonMapperException("Failed to execute jsonpath " + jsonPathExpression, e);
        }

        return result;
    }




    /**
     * Try to parse a string value to the Java type based on Schema type.
     *
     * @param fieldType the schema type
     * @param value     the string value
     * @return the result of parsing. In case of Exception (for ex NumberFormatException) the result is null.
     */
    public static Object parseValue(Schema.Type fieldType, String value) {
        Object result;
        try {
            result = switch (fieldType) {
                case STRING -> value;
                case INT -> Integer.valueOf(value);
                case LONG -> Long.valueOf(value);
                case FLOAT -> Float.valueOf(value);
                case DOUBLE -> Double.valueOf(value);
                case BOOLEAN -> Boolean.valueOf(value);
                default -> null;
            };
        } catch (Exception e) {
            result = null;
        }
        return result;
    }


    /**
     * Frequently the type is defined in avsc with this pattern : "type" : [ "null", "realType"] to allow a null value.
     * This pattern creates a UNION type, with two sub-types. This method extracts the non-null type ("real type").
     *
     * @param schema the schema node wich can be a UNION
     * @return the non-null type
     */
    public static Schema extractRealType(Schema schema) {
        return schema.getType() != Schema.Type.UNION ?
                schema :
                schema.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .findFirst().get();
    }
}
