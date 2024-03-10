package com.michelin.avroxmlmapper.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.NotImplementedException;

import java.util.*;

import static com.michelin.avroxmlmapper.utility.GenericUtils.extractRealType;

/**
 * Utility class for Avro to JSON conversion
 */
public final class AvroToJsonUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Create a JsonNode from a SpecificRecordBase, using jsonpath property (Avro model) to build the Json structure.
     *
     * @param record           the global SpecificRecordBase containing the entire data to parse in JSON
     * @param jsonpathSelector Name of the variable defining the xpath of the avsc file that needs to be used
     * @return the document produced
     */
    public static JsonNode createDocumentFromAvro(SpecificRecordBase record, String jsonpathSelector) {
        ObjectMapper mapper = new ObjectMapper();
        var document = mapper.createObjectNode();

        String rootElementsJsonPath = record.getSchema().getProp(jsonpathSelector);

        ObjectNode effectiveRootObject = findOrCreateObject(rootElementsJsonPath, document);


        AvroToJsonUtils.buildChildNodes(record, effectiveRootObject, jsonpathSelector);

        return document;
    }


    public static ObjectNode findOrCreateObject(String jsonPath, ObjectNode currentNode) {

        String[] jsonLevels = jsonPath.split("\\.");

        ObjectNode newEffectiveNode = currentNode;

        for (int i = 0; i < (jsonLevels.length); i++) {
            var searchedNode = newEffectiveNode.get(jsonLevels[i]);

            if (searchedNode == null) {
                searchedNode = mapper.createObjectNode();
                newEffectiveNode.set(jsonLevels[i], searchedNode);
            }

            newEffectiveNode = (ObjectNode) searchedNode;
        }
        return newEffectiveNode;
    }

    public static ObjectNode findOrCreateField(String jsonPath, ObjectNode currentNode) {

        String[] jsonLevels = jsonPath.split("\\.");

        ObjectNode newEffectiveNode = currentNode;

        if(jsonLevels.length == 1){
            return newEffectiveNode;
        }

        for (int i = 0; i < (jsonLevels.length-1); i++) {
            var searchedNode = newEffectiveNode.get(jsonLevels[i]);

            if (searchedNode == null) {
                searchedNode = mapper.createObjectNode();
                newEffectiveNode.set(jsonLevels[i], searchedNode);
            }

            newEffectiveNode = (ObjectNode) searchedNode;
        }
        return newEffectiveNode;
    }

    /**
     * Build all child nodes of an element (with type record in avsc) and return it as list.
     *
     * @param record           the record corresponding to the parent element
     * @param currentNode      the target document (necessary to create nodes)
     * @param jsonpathSelector Name of the variable defining the jsonpath of the avsc file that needs to be used
     */
    private static void buildChildNodes(SpecificRecordBase record, ObjectNode currentNode, String jsonpathSelector) {
        Map<String, JsonNode> childNodes = new HashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Schema fieldType = extractRealType(field.schema());
            String jsonPath;
            switch (fieldType.getType()) {
                case NULL:
                case UNION:
                case ENUM:
                    break;
                case RECORD:
                    jsonPath = field.getProp(jsonpathSelector);
                    if (jsonPath != null) {
                        var subRecord = (SpecificRecordBase) record.get(field.name());
                        if (subRecord != null) {
                            ObjectNode node = mapper.createObjectNode();
                            currentNode.set(field.name(), node);
                            buildChildNodes(subRecord, node, jsonpathSelector);
                        }
                    }
                    break;
                case ARRAY:
                    Schema elementSchema = fieldType.getElementType();
                    jsonPath = field.getProp(jsonpathSelector);
                    ArrayNode arrayNode = mapper.createArrayNode();

                    if (jsonPath != null) {
                        var list = (List) record.get(field.name());
                        if (list != null && !list.isEmpty()) {
                            if (extractRealType(elementSchema).getType() == Schema.Type.RECORD) { // an array of records
                                for (SpecificRecordBase item : (List<SpecificRecordBase>) list) {
                                    ObjectNode node = mapper.createObjectNode();
                                    arrayNode.add(node);
                                    buildChildNodes(item, node, jsonpathSelector);
                                }
                            } else if (extractRealType(elementSchema).getType() == Schema.Type.STRING) { // an array of string
                                for (String value : (List<String>) list) {
                                    arrayNode.add(value);
                                }
                            } else {
                                throw new NotImplementedException("Array implementation with value types other than records or String are not yet supported");
                            }
                        }
                    }
                    break;
                case MAP:
                    throw new NotImplementedException("Map Are not yet supported");
                default:
                    // all other = primitive types
                    var xpathList = getJsonpathList(field, jsonpathSelector);

                    String fieldValue = record.get(field.name()) != null ? record.get(field.name()).toString() : "";
                    if (!fieldValue.isEmpty()) {
                        xpathList.forEach(x -> {
                            currentNode.put(x, fieldValue);
                        });
                    }
            }
        }
    }


    private static List<String> getJsonpathList(Schema.Field field, String xpathSelector) {
        Object jsonpath1 = field.getObjectProp(xpathSelector);
        var xpathList = new ArrayList<String>();

        if (jsonpath1 == null || JsonProperties.NULL_VALUE.equals(jsonpath1)) {
            return xpathList;
        }

        //test if xpath is an array
        if (jsonpath1.getClass().getSimpleName().equals("ArrayList")) {
            xpathList.addAll((Collection<? extends String>) jsonpath1);
        } else {
            xpathList.add((String) jsonpath1);
        }

        return xpathList;
    }
}
