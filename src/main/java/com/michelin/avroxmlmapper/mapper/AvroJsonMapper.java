package com.michelin.avroxmlmapper.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.specific.SpecificRecordBase;

import static com.michelin.avroxmlmapper.constants.AvroJsonMapperConstants.JSONPATH_DEFAULT;
import static com.michelin.avroxmlmapper.mapper.AvroToJsonUtils.createDocumentFromAvro;
import static com.michelin.avroxmlmapper.utility.GenericUtils.jsonnodeToString;
import static com.michelin.avroxmlmapper.utility.GenericUtils.stringToDocument;


/**
 * Utility Class for JSON parsing (JSONPATH)
 */
public final class AvroJsonMapper {

    /* *************************************************** */
    /* Build an Avro from an XML document in string Format */
    /* *************************************************** */

    /**
     * <p>Converts an JSON string into a SpecificRecordBase object. The mapping is based on the "jsonpath" property defined for each of the fields in the original avsc file.</p>
     * <p>See README.md for more details.</p>
     *
     * @param stringDocument The XML string to convert
     * @param clazz          The Avro object to convert to
     * @param <T>            The type of the Avro object
     * @return The SpecificRecordBase object.
     */
    public static <T extends SpecificRecordBase> T convertJsonStringToAvro(String stringDocument, String baseNamespace, Class<T> clazz) {
        var document = stringToDocument(stringDocument);
        return JsonToAvroUtils.convert(document, baseNamespace, clazz, JSONPATH_DEFAULT);
    }

    /**
     * Converts an XML string into a SpecificRecordBase object. The mapping is based on the chosen xpathSelector property defined for each of the fields in the original avsc file. See README.md for more details.
     *
     * @param stringDocument        The XML string to convert
     * @param clazz                 The Avro object to convert to
     * @param jsonpathSelector         The jsonpathSelector property used to search for the xpathMapping in the Avro definition
     * @param <T>                   The type of the Avro object
     * @return the SpecificRecordBase object.
     */
    public static <T extends SpecificRecordBase> T convertJsonStringToAvro(String stringDocument, String baseNamespace, Class<T> clazz, String jsonpathSelector) {
        var document = stringToDocument(stringDocument);
        return JsonToAvroUtils.convert(document, baseNamespace, clazz, jsonpathSelector);
    }

    /* *************************************************** */
    /* Build an XML document in String format from an Avro */
    /* *************************************************** */

    /**
     * Create an JSON in String format from a SpecificRecordBase, using default "jsonpath" properties defined in the Avro model to build the JSON structure.
     *
     * @param record The SpecificRecordBase containing the entire data to parse in JSON
     * @return The JSON in String format
     */
    public static String convertAvroToJsonString(SpecificRecordBase record) {
        return jsonnodeToString(createDocumentFromAvro(record, JSONPATH_DEFAULT));
    }

    /**
     * Create an XML in String format from a SpecificRecordBase, using the provided xpathSelector defined in the Avro model to build the XML structure.
     *
     * @param record        The SpecificRecordBase containing the entire data to parse in JSON
     * @param xpathSelector Name of the variable defining the xpath of the avsc file that needs to be used
     * @return The JSON in String format
     */
    public static String convertAvroToJsonString(SpecificRecordBase record, String xpathSelector) {
        return jsonnodeToString(createDocumentFromAvro(record, xpathSelector));
    }

    /* ********************************** */
    /* Build an XML document from an Avro */
    /* ********************************** */

    /**
     * Create a Document from a SpecificRecordBase, using default "xpath" properties defined in the Avro model to build the XML structure.
     *
     * @param record The global SpecificRecordBase containing the entire data to parse in XML
     * @return The document produced
     */
    public static JsonNode convertAvroToJsonNode(SpecificRecordBase record) {
        return createDocumentFromAvro(record, JSONPATH_DEFAULT);
    }

    /**
     * Create a Document from a SpecificRecordBase, using xpath property (Avro model) to build the JSON structure.
     *
     * @param record        The SpecificRecordBase containing the entire data to parse in JSON
     * @param xpathSelector Name of the variable defining the xpath of the avsc file that needs to be used
     * @return The document produced
     */
    public static JsonNode convertAvroToJsonNode(SpecificRecordBase record, String xpathSelector) {
        return createDocumentFromAvro(record, xpathSelector);
    }

}
