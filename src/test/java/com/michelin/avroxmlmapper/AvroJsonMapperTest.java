package com.michelin.avroxmlmapper;


import com.michelin.avroxmlmapper.mapper.AvroJsonMapper;
import io.confluent.ps.demo.*;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroJsonMapperTest {


    @Test
    void testJsonToAvro() throws Exception {
        var input = IOUtils.toString(Objects.requireNonNull(AvroJsonMapperTest.class.getResourceAsStream("/basic.json")), StandardCharsets.UTF_8);

        var result = AvroJsonMapper.convertJsonStringToAvro(input, "io.confluent.ps.demo", TestBasicRecord.class);

        var expectedModel = buildDefaultModel();

        assertEquals(expectedModel, result);
    }

    @Test
    void testJsonToAvro_skipLevels() throws Exception {
        var input = IOUtils.toString(Objects.requireNonNull(AvroJsonMapperTest.class.getResourceAsStream("/skipLevel.json")), StandardCharsets.UTF_8);

        var result = AvroJsonMapper.convertJsonStringToAvro(input, "io.confluent.ps.demo", TestSkipLevelRecord.class);

        var expectedModel = TestSkipLevelRecord.newBuilder()
                .setEmbeddedRecord(EmbeddedRecord2.newBuilder()
                        .setField1("field1-value")
                        .setField2("field2-value")
                        .build())
                .build();

        assertEquals(expectedModel, result);
    }


    @Test
    void testJsonToAvro_arrays() throws Exception {
        var input = IOUtils.toString(Objects.requireNonNull(AvroJsonMapperTest.class.getResourceAsStream("/arrays.json")), StandardCharsets.UTF_8);

        var result = AvroJsonMapper.convertJsonStringToAvro(input, "io.confluent.ps.demo", TestArrayRecord.class);

        var expectedModel = TestArrayRecord.newBuilder()
                .setEmbeddedRecord(EmbeddedRecord3.newBuilder()
                        .setField1Values(List.of("field1-value1", "field1-value2"))
                        .setField2("field2-value")
                        .build())
                .build();

        assertEquals(expectedModel, result);
    }





    private TestBasicRecord buildDefaultModel() {

        return TestBasicRecord.newBuilder()
                .setEmbeddedRecord(EmbeddedRecord.newBuilder()
                        .setStringField("stringField")
                        .setOtherStringField("otherStringField")
                        .build())
                .build();

    }
}
