/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.CountingInputStream;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class XContentRecordReaderTests extends ESTestCase {
    public void testRead() throws JsonParseException, IOException {
        String data = "{\"a\":10, \"b\":20, \"c\":30}\n{\"b\":21, \"a\":11, \"c\":31}\n";
        XContentParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMap();

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));

        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        assertEquals(3, reader.read(record, gotFields));
        assertEquals("10", record[0]);
        assertEquals("20", record[1]);
        assertEquals("30", record[2]);

        assertEquals(3, reader.read(record, gotFields));
        assertEquals("11", record[0]);
        assertEquals("21", record[1]);
        assertEquals("31", record[2]);

        assertEquals(-1, reader.read(record, gotFields));
    }


    public void testRead_GivenNestedField() throws JsonParseException, IOException {
        String data = "{\"a\":10, \"b\":20, \"c\":{\"d\":30, \"e\":40}}";
        XContentParser parser = createParser(data);
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put("a", 0);
        fieldMap.put("b", 1);
        fieldMap.put("c.e", 2);

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));

        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        assertEquals(4, reader.read(record, gotFields));
        assertEquals("10", record[0]);
        assertEquals("20", record[1]);
        assertEquals("40", record[2]);

        assertEquals(-1, reader.read(record, gotFields));
    }


    public void testRead_GivenSingleValueArrays() throws JsonParseException, IOException {
        String data = "{\"a\":[10], \"b\":20, \"c\":{\"d\":30, \"e\":[40]}}";
        XContentParser parser = createParser(data);
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put("a", 0);
        fieldMap.put("b", 1);
        fieldMap.put("c.e", 2);

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));

        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        assertEquals(4, reader.read(record, gotFields));
        assertEquals("10", record[0]);
        assertEquals("20", record[1]);
        assertEquals("40", record[2]);

        assertEquals(-1, reader.read(record, gotFields));
    }


    public void testRead_GivenMultiValueArrays() throws JsonParseException, IOException {
        String data = "{\"a\":[10, 11], \"b\":20, \"c\":{\"d\":30, \"e\":[40, 50]}, "
                + "\"f\":[\"a\", \"a\", \"a\", \"a\"], \"g\":20}";
        XContentParser parser = createParser(data);
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put("a", 0);
        fieldMap.put("g", 1);
        fieldMap.put("c.e", 2);

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));

        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        assertEquals(6, reader.read(record, gotFields));
        assertEquals("10,11", record[0]);
        assertEquals("20", record[1]);
        assertEquals("40,50", record[2]);

        assertEquals(-1, reader.read(record, gotFields));
    }

    /**
     * There's a problem with the parser where in this case it skips over the
     * first 2 records instead of to the end of the first record which is
     * invalid json. This means we miss the next record after a bad one.
     */

    public void testRead_RecoverFromBadJson() throws JsonParseException, IOException {
        // no opening '{'
        String data = "\"a\":10, \"b\":20, \"c\":30}\n{\"b\":21, \"a\":11, \"c\":31}\n"
                + "{\"c\":32, \"b\":22, \"a\":12}";
        XContentParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMap();

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));

        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        assertEquals(0, reader.read(record, gotFields));
        assertEquals(3, reader.read(record, gotFields));
        assertEquals("12", record[0]);
        assertEquals("22", record[1]);
        assertEquals("32", record[2]);

        assertEquals(-1, reader.read(record, gotFields));
    }


    public void testRead_RecoverFromBadNestedJson() throws JsonParseException, IOException {
        // nested object 'd' is missing a ','
        String data = "{\"a\":10, \"b\":20, \"c\":30}\n"
                + "{\"b\":21, \"d\" : {\"ee\": 1 \"ff\":0}, \"a\":11, \"c\":31}";
        XContentParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMap();

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));

        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        // reads first object ok
        assertEquals(3, reader.read(record, gotFields));
        // skips to the end of the 2nd after reading 2 fields
        assertEquals(2, reader.read(record, gotFields));
        assertEquals("", record[0]);
        assertEquals("21", record[1]);
        assertEquals("", record[2]);

        assertEquals(-1, reader.read(record, gotFields));
    }


    public void testRead_HitParseErrorsLimit() throws JsonParseException, IOException {
        // missing a ':'
        String format = "{\"a\":1%1$d, \"b\"2%1$d, \"c\":3%1$d}\n";
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < XContentRecordReader.PARSE_ERRORS_LIMIT; i++) {
            builder.append(String.format(Locale.ROOT, format, i));
        }

        XContentParser parser = createParser(builder.toString());
        Map<String, Integer> fieldMap = createFieldMap();

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));
        ESTestCase.expectThrows(ElasticsearchParseException.class, () -> readUntilError(reader));
    }

    private void readUntilError(XContentRecordReader reader) throws IOException {
        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        // this should throw after PARSE_ERRORS_LIMIT errors
        for (int i = 0; i < XContentRecordReader.PARSE_ERRORS_LIMIT; i++) {
            reader.read(record, gotFields);
        }
    }

    public void testRead_givenControlCharacterInData() throws Exception {
        char controlChar = '\u0002';

        String data = "{\"a\":10, \"" + controlChar + "\" : 5, \"b\":20, \"c\":30}"
                + "\n{\"b\":21, \"a\":11, \"c\":31}" + "\n{\"c\":32, \"b\":22, \"a\":12}\n";

        XContentParser parser = createParser(data);
        Map<String, Integer> fieldMap = createFieldMap();

        XContentRecordReader reader = new XContentRecordReader(parser, fieldMap,
                mock(Logger.class));

        String record[] = new String[3];
        boolean gotFields[] = new boolean[3];

        reader.read(record, gotFields);
        assertEquals(3, reader.read(record, gotFields));
        assertEquals(3, reader.read(record, gotFields));
    }

    private XContentParser createParser(String input) throws JsonParseException, IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(
                input.getBytes(StandardCharsets.UTF_8));
        InputStream inputStream2 = new CountingInputStream(inputStream,
                mock(DataCountsReporter.class));
        return XContentFactory.xContent(XContentType.JSON)
                .createParser(new NamedXContentRegistry(Collections.emptyList()),
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputStream2);
    }

    private Map<String, Integer> createFieldMap() {
        Map<String, Integer> fieldMap = new HashMap<>();
        fieldMap.put("a", 0);
        fieldMap.put("b", 1);
        fieldMap.put("c", 2);
        return fieldMap;
    }

}
