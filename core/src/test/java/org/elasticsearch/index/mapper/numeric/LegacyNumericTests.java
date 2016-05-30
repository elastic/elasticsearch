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

import org.apache.lucene.analysis.LegacyNumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.LegacyFloatFieldMapper;
import org.elasticsearch.index.mapper.core.LegacyLongFieldMapper;
import org.elasticsearch.index.mapper.core.LegacyNumberFieldMapper;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.mapper.string.SimpleStringMappingTests;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class LegacyNumericTests extends ESSingleNodeTestCase {

    private static final Settings BW_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0).build();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testIgnoreMalformedOption() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field1").field("type", "integer").field("ignore_malformed", true).endObject()
                    .startObject("field2").field("type", "integer").field("ignore_malformed", false).endObject()
                    .startObject("field3").field("type", "integer").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "a")
                .field("field2", "1")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field1"), nullValue());
        assertThat(doc.rootDoc().getField("field2"), notNullValue());

        try {
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }

        // Verify that the default is false
        try {
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field3", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }

        // Unless the global ignore_malformed option is set to true
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0)
                .put("index.mapping.ignore_malformed", true).build();
        defaultMapper = createIndex("test2", indexSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field3", "a")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field3"), nullValue());

        // This should still throw an exception, since field2 is specifically set to ignore_malformed=false
        try {
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }
    }

    public void testCoerceOption() throws Exception {
        String [] nonFractionNumericFieldTypes={"integer","long","short"};
        //Test co-ercion policies on all non-fraction numerics
        DocumentMapperParser parser = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser();
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

            DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

            //Test numbers passed as strings
            String invalidJsonNumberAsString="1";
            ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
            doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
            doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
                defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
            doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
                defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
                .startObject("int1")
                    .field("type", "integer")
                .endObject()
                .startObject("int2")
                    .field("type", "integer")
                    .field("index", "no")
                .endObject()
                .startObject("double1")
                    .field("type", "double")
                .endObject()
                .startObject("double2")
                    .field("type", "integer")
                    .field("index", "no")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("int1", "1234")
                .field("double1", "1234")
                .field("int2", "1234")
                .field("double2", "1234")
                .endObject()
                .bytes());
        Document doc = parsedDoc.rootDoc();
        assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "int1"));
        assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "double1"));
        assertEquals(DocValuesType.NONE, SimpleStringMappingTests.docValuesType(doc, "int2"));
        assertEquals(DocValuesType.NONE, SimpleStringMappingTests.docValuesType(doc, "double2"));
    }

    public void testUnIndex() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("int")
                    .field("type", "integer")
                    .field("index", false)
                .endObject()
                .startObject("double")
                    .field("type", "double")
                    .field("index", false)
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        assertEquals("{\"type\":{\"properties\":{\"double\":{\"type\":\"double\",\"index\":false},\"int\":{\"type\":\"integer\",\"index\":false}}}}",
                defaultMapper.mapping().toString());

        ParsedDocument parsedDoc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("int", "1234")
                .field("double", "1234")
                .endObject()
                .bytes());
        final Document doc = parsedDoc.rootDoc();
        for (IndexableField field : doc.getFields("int")) {
            assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        }
        for (IndexableField field : doc.getFields("double")) {
            assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        }
    }

    public void testBwCompatIndex() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("int")
                    .field("type", "integer")
                    .field("index", "no")
                .endObject()
                .startObject("double")
                    .field("type", "double")
                    .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        Settings oldSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_2_0).build();
        DocumentMapper defaultMapper = createIndex("test", oldSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertEquals("{\"type\":{\"properties\":{\"double\":{\"type\":\"double\"},\"int\":{\"type\":\"integer\",\"index\":false}}}}",
                defaultMapper.mapping().toString());
    }

    public void testDocValuesOnNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("int")
                            .field("type", "integer")
                            .field("doc_values", true)
                        .endObject()
                        .startObject("double")
                            .field("type", "double")
                            .field("doc_values", true)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
    public void testPrecisionStepDefaultsDetected() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("numeric_detection", true)
                .field("date_detection", true)
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("long",   "100")
                .field("double", "100.0")
                .field("date",   "2010-01-01")
                .endObject()
                .bytes());

        assertEquals(1, doc.docs().size());
        Document luceneDoc = doc.docs().get(0);

        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("long"));
        assertThat(luceneDoc.getField("double").numericValue(), instanceOf(Float.class));
        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_32_BIT, luceneDoc.getField("double"));
        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("date"));
    }

    /** Test default precision step for numeric types */
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

        DocumentMapper mapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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

        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("long"));
        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("double"));
        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("date"));
        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("ip"));

        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_32_BIT, luceneDoc.getField("int"));
        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_32_BIT, luceneDoc.getField("float"));

        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_16_BIT, luceneDoc.getField("short"));
        assertPrecisionStepEquals(LegacyNumberFieldMapper.Defaults.PRECISION_STEP_8_BIT,  luceneDoc.getField("byte"));
    }

    /** Test precision step set to silly explicit values */
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

        DocumentMapper mapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
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
        assertThat(ts, instanceOf(LegacyNumericTokenStream.class));
        assertEquals(expected, ((LegacyNumericTokenStream)ts).getPrecisionStep());
    }

    public void testTermVectorsBackCompat() throws Exception {
        for (String type : Arrays.asList("byte", "short", "integer", "long", "float", "double")) {
            doTestTermVectorsBackCompat(type);
        }
    }

    private void doTestTermVectorsBackCompat(String type) throws Exception {
        DocumentMapperParser parser = createIndex("index-" + type).mapperService().documentMapperParser();
        String mappingWithTV = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                        .field("term_vector", "yes")
                    .endObject()
                .endObject().endObject().endObject().string();
        try {
            parser.parse("type", new CompressedXContent(mappingWithTV));
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Mapping definition for [foo] has unsupported parameters:  [term_vector : yes]"));
        }

        Settings oldIndexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_1_0)
                .build();
        parser = createIndex("index2-" + type, oldIndexSettings).mapperService().documentMapperParser();
        parser.parse("type", new CompressedXContent(mappingWithTV)); // no exception
    }

    public void testAnalyzerBackCompat() throws Exception {
        for (String type : Arrays.asList("byte", "short", "integer", "long", "float", "double")) {
            doTestAnalyzerBackCompat(type);
        }
    }

    private void doTestAnalyzerBackCompat(String type) throws Exception {
        DocumentMapperParser parser = createIndex("index-" + type).mapperService().documentMapperParser();
        String mappingWithTV = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                        .field("analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject().string();
        try {
            parser.parse("type", new CompressedXContent(mappingWithTV));
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Mapping definition for [foo] has unsupported parameters:  [analyzer : keyword]"));
        }

        Settings oldIndexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_1_0)
                .build();
        parser = createIndex("index2-" + type, oldIndexSettings).mapperService().documentMapperParser();
        parser.parse("type", new CompressedXContent(mappingWithTV)); // no exception
    }


    public void testIgnoreFielddata() throws IOException {
        for (String type : Arrays.asList("byte", "short", "integer", "long", "float", "double")) {
            Settings oldIndexSettings = Settings.builder()
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_1_0)
                    .build();
            DocumentMapperParser parser = createIndex("index-" + type, oldIndexSettings).mapperService().documentMapperParser();
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                        .startObject("fielddata")
                            .field("loading", "eager")
                        .endObject()
                    .endObject()
                .endObject().endObject().endObject().string();
            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
            String expectedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                    .endObject()
                .endObject().endObject().endObject().string();
            assertEquals(expectedMapping, mapper.mappingSource().string());
        }
    }
}
