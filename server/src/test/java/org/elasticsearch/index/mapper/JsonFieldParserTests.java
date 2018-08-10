/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.JsonFieldMapper.JsonFieldType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

public class JsonFieldParserTests extends ESTestCase {
    private JsonFieldParser parser;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MappedFieldType fieldType = new JsonFieldType();
        fieldType.setName("field");
        parser = new JsonFieldParser(fieldType);
    }

    public void testTextValues() throws Exception {
        String input = "{ \"key1\": \"value1\", \"key2\": \"value2\" }";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(2, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("value1"), field1.binaryValue());

        IndexableField field2 = fields.get(1);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("value2"), field2.binaryValue());
    }

    public void testNumericValues() throws Exception {
        String input = "{ \"key\": 2.718 }";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(1, fields.size());

        IndexableField field = fields.get(0);
        assertEquals("field", field.name());
        assertEquals(new BytesRef("2.718"), field.binaryValue());
    }

    public void testBooleanValues() throws Exception {
        String input = "{ \"key\": false }";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(1, fields.size());

        IndexableField field = fields.get(0);
        assertEquals("field", field.name());
        assertEquals(new BytesRef("false"), field.binaryValue());
    }

    public void testArrays() throws Exception {
        String input = "{ \"key\": [true, false] }";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(2, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("true"), field1.binaryValue());

        IndexableField field2 = fields.get(1);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("false"), field2.binaryValue());
    }

    public void testNestedObjects() throws Exception {
        String input = "{ \"parent1\": { \"key\" : \"value\" }," +
            "\"parent2\": { \"key\" : \"value\" }}";
        XContentParser xContentParser = createXContentParser(input);

        List<IndexableField> fields = parser.parse(xContentParser);
        assertEquals(2, fields.size());

        IndexableField field1 = fields.get(0);
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("value"), field1.binaryValue());

        IndexableField field2 = fields.get(1);
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("value"), field2.binaryValue());
    }

    private XContentParser createXContentParser(String input) throws IOException {
        XContentParser xContentParser = createParser(JsonXContent.jsonXContent, input);
        xContentParser.nextToken();
        return xContentParser;
    }
}
