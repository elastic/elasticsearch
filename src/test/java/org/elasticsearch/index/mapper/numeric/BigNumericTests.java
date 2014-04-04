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

package org.elasticsearch.index.mapper.numeric;

import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.BigDecimalFieldMapper;
import org.elasticsearch.index.mapper.core.BigIntegerFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.string.SimpleStringMappingTests;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class BigNumericTests extends ElasticsearchTestCase {

    @Test
    public void testBigNumericDetectionEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("lossless_numeric_detection", true)
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_biginteger", "12345678901234567890123456789123456789012345678901234567890")
                .field("s_bigdecimal", "12345678901234567890123456789123456789012345678901234567890.90")
                .endObject()
                .bytes());

        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_biginteger");
        assertThat(mapper, instanceOf(BigIntegerFieldMapper.class));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_bigdecimal");
        assertThat(mapper, instanceOf(BigDecimalFieldMapper.class));
    }

    @Test
    public void testBigNumericDetectionDefault() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_biginteger", "12345678901234567890123456789123456789012345678901234567890")
                .field("s_bigdecimal", "12345678901234567890123456789123456789012345678901234567890.90")
                .endObject()
                .bytes());

        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_biginteger");
        assertThat(mapper, instanceOf(StringFieldMapper.class));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_bigdecimal");
        assertThat(mapper, instanceOf(StringFieldMapper.class));
    }

    @Test
    public void testIgnoreMalformedOption() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field1").field("type", "biginteger").field("ignore_malformed", true).endObject()
                    .startObject("field2").field("type", "biginteger").field("ignore_malformed", false).endObject()
                    .startObject("field3").field("type", "biginteger").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "a")
                .field("field2", "1")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field1"), nullValue());
        assertThat(doc.rootDoc().getField("field2"), notNullValue());

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }

        // Verify that the default is false
        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field3", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }

        // Unless the global ignore_malformed option is set to true
        Settings indexSettings = settingsBuilder().put("index.mapping.ignore_malformed", true).build();
        defaultMapper = MapperTestUtils.newParser(indexSettings).parse(mapping);
        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field3", "a")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field3"), nullValue());

        // This should still throw an exception, since field2 is specifically set to ignore_malformed=false
        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }
    }

    @Test
    public void testCoerceOption() throws Exception {
        String [] nonFractionNumericFieldTypes={"biginteger"};
        //Test co-ercion policies on all non-fraction numerics
        for (String nonFractionNumericFieldType : nonFractionNumericFieldTypes) {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                    .startObject("properties")
                        .startObject("noErrorNoCoerceField").field("type", nonFractionNumericFieldType).field("ignore_malformed", true)
                            .field("coerce", false).endObject()
                        .startObject("noErrorCoerceField").field("type", nonFractionNumericFieldType).field("ignore_malformed", true)
                            .field("coerce", true).endObject()
                        .startObject("errorDefaultCoerce").field("type", nonFractionNumericFieldType).field("ignore_malformed", false).endObject()
                        .startObject("errorNoCoerce").field("type", nonFractionNumericFieldType).field("ignore_malformed", false)
                            .field("coerce", false).endObject()
                    .endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

            //Test numbers passed as strings
            String invalidJsonNumberAsString="1";
            ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("noErrorNoCoerceField", invalidJsonNumberAsString)
                    .field("noErrorCoerceField", invalidJsonNumberAsString)
                    .field("errorDefaultCoerce", invalidJsonNumberAsString)
                    .endObject()
                    .bytes());
            assertThat(doc.rootDoc().getField("noErrorNoCoerceField"), nullValue());
            assertThat(doc.rootDoc().getField("noErrorCoerceField"), notNullValue());
            //Default is ignore_malformed=true and coerce=true
            assertThat(doc.rootDoc().getField("errorDefaultCoerce"), notNullValue());
            
            //Test valid case of numbers passed as numbers
            int validNumber=1;
            doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("noErrorNoCoerceField", validNumber)
                    .field("noErrorCoerceField", validNumber)
                    .field("errorDefaultCoerce", validNumber)
                    .endObject()
                    .bytes());
            assertEquals(validNumber,doc.rootDoc().getField("noErrorNoCoerceField").numericValue().intValue());
            assertEquals(validNumber,doc.rootDoc().getField("noErrorCoerceField").numericValue().intValue());
            assertEquals(validNumber,doc.rootDoc().getField("errorDefaultCoerce").numericValue().intValue());
            
            //Test valid case of negative numbers passed as numbers
            int validNegativeNumber=-1;
            doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("noErrorNoCoerceField", validNegativeNumber)
                    .field("noErrorCoerceField", validNegativeNumber)
                    .field("errorDefaultCoerce", validNegativeNumber)
                    .endObject()
                    .bytes());
            assertEquals(validNegativeNumber,doc.rootDoc().getField("noErrorNoCoerceField").numericValue().intValue());
            assertEquals(validNegativeNumber,doc.rootDoc().getField("noErrorCoerceField").numericValue().intValue());
            assertEquals(validNegativeNumber,doc.rootDoc().getField("errorDefaultCoerce").numericValue().intValue());
            

            try {
                defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                        .startObject()
                        .field("errorNoCoerce", invalidJsonNumberAsString)
                        .endObject()
                        .bytes());
            } catch (MapperParsingException e) {
                assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            }
            
            
            //Test questionable case of floats passed to ints
            float invalidJsonForInteger=1.9f;
            int coercedFloatValue=1; //This is what the JSON parser will do to a float - truncate not round
            doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("noErrorNoCoerceField", invalidJsonForInteger)
                    .field("noErrorCoerceField", invalidJsonForInteger)
                    .field("errorDefaultCoerce", invalidJsonForInteger)
                    .endObject()
                    .bytes());

            assertThat(doc.rootDoc().getField("noErrorNoCoerceField"), nullValue());
            assertEquals(coercedFloatValue, doc.rootDoc().getField("noErrorCoerceField").numericValue().intValue());
            //Default is ignore_malformed=true and coerce=true
            assertEquals(coercedFloatValue, doc.rootDoc().getField("errorDefaultCoerce").numericValue().intValue());
            
            try {
                defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                        .startObject()
                        .field("errorNoCoerce", invalidJsonForInteger)
                        .endObject()
                        .bytes());
            } catch (MapperParsingException e) {
                assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            }
        }
    }
    
    
    public void testDocValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("bigint")
                    .field("type", "biginteger")
                    .startObject("fielddata")
                        .field("format", "doc_values")
                    .endObject()
                .endObject()
                .startObject("bigdec")
                    .field("type", "bigdecimal")
                    .startObject("fielddata")
                        .field("format", "doc_values")
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument parsedDoc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("bigint", "1234")
                .field("bigdec", "1234")
                .endObject()
                .bytes());
        final Document doc = parsedDoc.rootDoc();
        assertEquals(DocValuesType.BINARY, SimpleStringMappingTests.docValuesType(doc, "bigint"));
        assertEquals(DocValuesType.BINARY, SimpleStringMappingTests.docValuesType(doc, "bigdec"));
    }

    public void testDocValuesOnNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("bigint")
                            .field("type", "biginteger")
                            .startObject("fielddata")
                                .field("format", "doc_values")
                            .endObject()
                        .endObject()
                        .startObject("bigdec")
                            .field("type", "bigdecimal")
                            .startObject("fielddata")
                                .field("format", "doc_values")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument parsedDoc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .startArray("nested")
                        .startObject()
                            .field("bigint", "1234")
                            .field("bigdec", "1234")
                        .endObject()
                        .startObject()
                            .field("bigint", "-1")
                            .field("bigdec", "-2")
                        .endObject()
                    .endArray()
                .endObject()
                .bytes());
        for (Document doc : parsedDoc.docs()) {
            if (doc == parsedDoc.rootDoc()) {
                continue;
            }
            assertEquals(DocValuesType.BINARY, SimpleStringMappingTests.docValuesType(doc, "nested.bigint"));
            assertEquals(DocValuesType.BINARY, SimpleStringMappingTests.docValuesType(doc, "nested.bigdec"));
        }
    }
}
