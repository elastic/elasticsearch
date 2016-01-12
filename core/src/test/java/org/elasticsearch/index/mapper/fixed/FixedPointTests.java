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

package org.elasticsearch.index.mapper.fixed;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.string.SimpleStringMappingTests;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.DoubleMatcher.nearlyEqual;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class FixedPointTests extends ESSingleNodeTestCase {

    public void testInvalidDecimalPlaces() throws Exception {
        IndexService index = createIndex("test");

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "fixed").field("decimal_places", 0).endObject()
            .endObject()
            .endObject().endObject().string();

        try {
            client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();
            fail("Should have thrown MapperParsingException[Property [decimal_places] must be greater than zero.]");
        } catch (MapperParsingException e) {
            assertEquals(e.getMessage(), "Property [decimal_places] must be greater than zero.");
        }

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "fixed").field("decimal_places", 20).endObject()
            .endObject()
            .endObject().endObject().string();

        try {
            client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();
            fail("Should have thrown MapperParsingException[Property [decimal_places] must be smaller than 19.]");
        } catch (MapperParsingException e) {
            assertEquals(e.getMessage(), "Property [decimal_places] must be smaller than 19.");
        }

    }

    public void testDecimalPlaces() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "fixed").field("decimal_places", 1).endObject()
            .startObject("field2").field("type", "fixed").field("decimal_places", 2).endObject()
            .startObject("field3").field("type", "fixed").field("decimal_places", 3).endObject()
            .startObject("field4").field("type", "fixed").endObject()    // default: 2
            .endObject()
            .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        double doubleValue = 1.2345;

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field1", doubleValue)
            .field("field2", doubleValue)
            .field("field3", doubleValue)
            .field("field4", doubleValue)
            .endObject()
            .bytes());

        assertTrue(nearlyEqual(1.2, doc.rootDoc().getField("field1").numericValue().doubleValue(), 0.01));
        assertTrue(nearlyEqual(1.23, doc.rootDoc().getField("field2").numericValue().doubleValue(), 0.01));
        assertTrue(nearlyEqual(1.234, doc.rootDoc().getField("field3").numericValue().doubleValue(), 0.01));
        assertTrue(nearlyEqual(1.23, doc.rootDoc().getField("field4").numericValue().doubleValue(), 0.01));
    }

    public void testIgnoreMalformedOption() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "fixed").field("ignore_malformed", true).endObject()
            .startObject("field2").field("type", "fixed").field("ignore_malformed", false).endObject()
            .startObject("field3").field("type", "fixed").endObject()
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
        Settings indexSettings = settingsBuilder().put("index.mapping.ignore_malformed", true).build();
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

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("noErrorNoCoerceField").field("type", "fixed").field("ignore_malformed", true)
            .field("coerce", false).endObject()
            .startObject("noErrorCoerceField").field("type", "fixed").field("ignore_malformed", true)
            .field("coerce", true).endObject()
            .startObject("errorDefaultCoerce").field("type", "fixed").field("ignore_malformed", false).endObject()
            .startObject("errorNoCoerce").field("type", "fixed").field("ignore_malformed", false)
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
        assertEquals(validNumber, doc.rootDoc().getField("noErrorNoCoerceField").numericValue().intValue());
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
        assertEquals(validNegativeNumber, doc.rootDoc().getField("noErrorNoCoerceField").numericValue().intValue());
        assertEquals(validNegativeNumber, doc.rootDoc().getField("noErrorCoerceField").numericValue().intValue());
        assertEquals(validNegativeNumber, doc.rootDoc().getField("errorDefaultCoerce").numericValue().intValue());


        try {
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("errorNoCoerce", invalidJsonNumberAsString)
                .endObject()
                .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        }

    }


    public void testDocValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("fixed")
            .field("type", "fixed")
            .startObject("fielddata")
            .field("format", "doc_values")
            .endObject()
            .endObject()
            .endObject()
            .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("fixed", "1234")
            .endObject()
            .bytes());
        final Document doc = parsedDoc.rootDoc();
        assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "fixed"));
    }

    public void testDocValuesOnNested() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("nested")
            .field("type", "nested")
            .startObject("properties")
            .startObject("fixed")
            .field("type", "fixed")
            .startObject("fielddata")
            .field("format", "doc_values")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .startArray("nested")
            .startObject()
            .field("fixed", "1234")
            .endObject()
            .startObject()
            .field("fixed", "0.23")
            .endObject()
            .startObject()
            .field("fixed", "-0.12")
            .endObject()
            .endArray()
            .endObject()
            .bytes());
        for (Document doc : parsedDoc.docs()) {
            if (doc == parsedDoc.rootDoc()) {
                continue;
            }
            assertEquals(DocValuesType.SORTED_NUMERIC, SimpleStringMappingTests.docValuesType(doc, "nested.fixed"));
        }
    }

    /** Test default precision step for numeric types */
    public void testPrecisionStepDefaultsMapped() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("fixed")
            .field("type", "fixed")
            .endObject()
            .endObject()
            .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("fixed", "34.545")
            .endObject()
            .bytes());

        assertEquals(1, doc.docs().size());
        Document luceneDoc = doc.docs().get(0);

        assertPrecisionStepEquals(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, luceneDoc.getField("fixed"));
    }

    /** Test precision step set to silly explicit values */
    public void testPrecisionStepExplicit() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("fixed")
            .field("type", "fixed")
            .field("precision_step", "2")
            .endObject()
            .endObject()
            .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("fixed", "34.545")
            .endObject()
            .bytes());

        assertEquals(1, doc.docs().size());
        Document luceneDoc = doc.docs().get(0);

        assertPrecisionStepEquals(2, luceneDoc.getField("fixed"));
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
        assertEquals(expected, ((NumericTokenStream) ts).getPrecisionStep());
    }

    public void testBasicSearch() throws Exception {
        IndexService index = createIndex("test");

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "fixed").field("decimal_places", 2).endObject()
            .endObject()
            .endObject().endObject().string();

        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();

        client().prepareIndex("test", "type", "1")
            .setSource("field1", 1.2345).setRefresh(true).execute().actionGet();

        // Check 1 decimal places, should not match
        SearchResponse search = client().prepareSearch()
            .setQuery(termQuery("field1", 1.2))
            .execute().actionGet();

        assertHitCount(search, 0l);

        // Check 2 decimal places
        search = client().prepareSearch()
            .setQuery(termQuery("field1", 1.23))
            .execute().actionGet();

        assertHitCount(search, 1l);
        assertEquals(search.getHits().getAt(0).getSource().get("field1"), 1.2345);

        // Check 3 decimal places -- should truncate down to 2
        search = client().prepareSearch()
            .setQuery(termQuery("field1", 1.239))
            .execute().actionGet();

        assertHitCount(search, 1l);
        assertEquals(search.getHits().getAt(0).getSource().get("field1"), 1.2345);

        // Check 4 decimal places -- should truncate down to 2
        search = client().prepareSearch()
            .setQuery(termQuery("field1", 1.2349))
            .execute().actionGet();

        assertHitCount(search, 1l);
        assertEquals(search.getHits().getAt(0).getSource().get("field1"), 1.2345);


        // Check ranges
        search = client().prepareSearch()
            .setQuery(rangeQuery("field1").gte(1.0).lte(2.0))
            .execute().actionGet();

        assertHitCount(search, 1l);
        assertEquals(search.getHits().getAt(0).getSource().get("field1"), 1.2345);

        search = client().prepareSearch()
            .setQuery(rangeQuery("field1").gte(1.23).lte(2.0))
            .execute().actionGet();

        assertHitCount(search, 1l);
        assertEquals(search.getHits().getAt(0).getSource().get("field1"), 1.2345);

        search = client().prepareSearch()
            .setQuery(rangeQuery("field1").gte(1.0).lte(1.23))
            .execute().actionGet();

        assertHitCount(search, 1l);
        assertEquals(search.getHits().getAt(0).getSource().get("field1"), 1.2345);

        search = client().prepareSearch()
            .setQuery(rangeQuery("field1").gte(1.2399).lte(2.0))    // extra decimal places truncated
            .execute().actionGet();

        assertHitCount(search, 1l);
        assertEquals(search.getHits().getAt(0).getSource().get("field1"), 1.2345);

        search = client().prepareSearch()
            .setQuery(rangeQuery("field1").gte(1.0).lte(1.2))    // not enough decimal places to match
            .execute().actionGet();

        assertHitCount(search, 0l);
    }
}
