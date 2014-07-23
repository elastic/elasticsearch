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

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.string.SimpleStringMappingTests;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 */
public class SimpleNumericTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testNumericDetectionEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("numeric_detection", true)
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_long", "100")
                .field("s_double", "100.0")
                .endObject()
                .bytes());

        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper, instanceOf(LongFieldMapper.class));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper, instanceOf(DoubleFieldMapper.class));
    }

    @Test
    public void testNumericDetectionDefault() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("s_long", "100")
                .field("s_double", "100.0")
                .endObject()
                .bytes());

        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper, instanceOf(StringFieldMapper.class));

        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper, instanceOf(StringFieldMapper.class));
    }

    @Test
    public void testIgnoreMalformedOption() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field1").field("type", "integer").field("ignore_malformed", true).endObject()
                    .startObject("field2").field("type", "integer").field("ignore_malformed", false).endObject()
                    .startObject("field3").field("type", "integer").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

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
        defaultMapper = createIndex("test2", indexSettings).mapperService().documentMapperParser().parse(mapping);
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
        String [] nonFractionNumericFieldTypes={"integer","long","short"};
        //Test co-ercion policies on all non-fraction numerics
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
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

            DocumentMapper defaultMapper = parser.parse(mapping);

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
            assertEquals(coercedFloatValue,doc.rootDoc().getField("noErrorCoerceField").numericValue().intValue());
            //Default is ignore_malformed=true and coerce=true
            assertEquals(coercedFloatValue,doc.rootDoc().getField("errorDefaultCoerce").numericValue().intValue());
            
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
                .startObject("int")
                    .field("type", "integer")
                    .startObject("fielddata")
                        .field("format", "doc_values")
                    .endObject()
                .endObject()
                .startObject("double")
                    .field("type", "double")
                    .startObject("fielddata")
                        .field("format", "doc_values")
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument parsedDoc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("int", "1234")
                .field("double", "1234")
                .endObject()
                .bytes());
        final Document doc = parsedDoc.rootDoc();
        assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "int"));
        assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "double"));
    }

    public void testDocValuesOnNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("int")
                            .field("type", "integer")
                            .startObject("fielddata")
                                .field("format", "doc_values")
                            .endObject()
                        .endObject()
                        .startObject("double")
                            .field("type", "double")
                            .startObject("fielddata")
                                .field("format", "doc_values")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument parsedDoc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .startArray("nested")
                        .startObject()
                            .field("int", "1234")
                            .field("double", "1234")
                        .endObject()
                        .startObject()
                            .field("int", "-1")
                            .field("double", "-2")
                        .endObject()
                    .endArray()
                .endObject()
                .bytes());
        for (Document doc : parsedDoc.docs()) {
            if (doc == parsedDoc.rootDoc()) {
                continue;
            }
            assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "nested.int"));
            assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "nested.double"));
        }
    }
    
    /** Test default precision step for autodetected numeric types */
    @Test
    public void testPrecisionStepDefaultsDetected() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("numeric_detection", true)
                .field("date_detection", true)
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("long",   "100")
                .field("double", "100.0")
                .field("date",   "2010-01-01")
                .endObject()
                .bytes());
        
        assertEquals(1, doc.docs().size());
        Document luceneDoc = doc.docs().get(0);
        
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("long"));
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("double"));
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("date"));
    }
    
    /** Test default precision step for numeric types */
    @Test
    public void testPrecisionStepDefaultsMapped() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("int")
                    .field("type", "integer")
                .endObject()
                .startObject("float")
                    .field("type", "float")
                .endObject()
                .startObject("long")
                    .field("type", "long")
                .endObject()
                .startObject("double")
                    .field("type", "double")
                .endObject()
                .startObject("short")
                    .field("type", "short")
                .endObject()
                .startObject("byte")
                    .field("type", "byte")
                .endObject()
                .startObject("date")
                    .field("type", "date")
                .endObject()
                .startObject("ip")
                    .field("type", "ip")
                .endObject()
                
                .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        
        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("int",    "100")
                .field("float",  "100.0")
                .field("long",   "5000")
                .field("double", "34.545")
                .field("short",  "1645")
                .field("byte",   "50")
                .field("date",   "2010-01-01")
                .field("ip",     "255.255.255.255")
                .endObject()
                .bytes());
        
        assertEquals(1, doc.docs().size());
        Document luceneDoc = doc.docs().get(0);
        
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("long"));
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("double"));
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("date"));
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("ip"));
        
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_32_BIT, luceneDoc.getField("int"));
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_32_BIT, luceneDoc.getField("float"));
        
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_16_BIT, luceneDoc.getField("short"));
        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_8_BIT,  luceneDoc.getField("byte"));
    }
    
    /** Test precision step set to silly explicit values */
    @Test
    public void testPrecisionStepExplicit() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("int")
                    .field("type", "integer")
                    .field("precision_step", "1")
                .endObject()
                .startObject("float")
                    .field("type", "float")
                    .field("precision_step", "2")
                .endObject()
                .startObject("long")
                    .field("type", "long")
                    .field("precision_step", "1")
                .endObject()
                .startObject("double")
                    .field("type", "double")
                    .field("precision_step", "2")
                .endObject()
                .startObject("short")
                    .field("type", "short")
                    .field("precision_step", "1")
                .endObject()
                .startObject("byte")
                    .field("type", "byte")
                    .field("precision_step", "2")
                .endObject()
                .startObject("date")
                    .field("type", "date")
                    .field("precision_step", "1")
                .endObject()
                .startObject("ip")
                    .field("type", "ip")
                    .field("precision_step", "2")
                .endObject()
                
                .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        
        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("int",    "100")
                .field("float",  "100.0")
                .field("long",   "5000")
                .field("double", "34.545")
                .field("short",  "1645")
                .field("byte",   "50")
                .field("date",   "2010-01-01")
                .field("ip",     "255.255.255.255")
                .endObject()
                .bytes());
        
        assertEquals(1, doc.docs().size());
        Document luceneDoc = doc.docs().get(0);
        
        assertPrecisionStepEquals(1, luceneDoc.getField("int"));
        assertPrecisionStepEquals(2, luceneDoc.getField("float"));
        assertPrecisionStepEquals(1, luceneDoc.getField("long"));
        assertPrecisionStepEquals(2, luceneDoc.getField("double"));
        assertPrecisionStepEquals(1, luceneDoc.getField("short"));
        assertPrecisionStepEquals(2, luceneDoc.getField("byte"));
        assertPrecisionStepEquals(1, luceneDoc.getField("date"));
        assertPrecisionStepEquals(2, luceneDoc.getField("ip"));

    }
    
    /** checks precisionstep on both the fieldtype and the tokenstream */
    private static void assertPrecisionStepEquals(int expected, IndexableField field) throws IOException {
        assertNotNull(field);
        assertThat(field, instanceOf(Field.class));
        
        // check fieldtype's precisionstep
        assertEquals(expected, ((Field)field).fieldType().numericPrecisionStep());
        
        // check the tokenstream actually used by the indexer
        TokenStream ts = field.tokenStream(null, null);
        assertThat(ts, instanceOf(NumericTokenStream.class)); 
        assertEquals(expected, ((NumericTokenStream)ts).getPrecisionStep());
    }
}
