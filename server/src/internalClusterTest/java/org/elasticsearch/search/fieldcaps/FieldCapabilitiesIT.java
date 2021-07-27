/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fieldcaps;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FieldCapabilitiesIT extends ESIntegTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        XContentBuilder oldIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("distance")
                            .field("type", "double")
                        .endObject()
                        .startObject("route_length_miles")
                            .field("type", "alias")
                            .field("path", "distance")
                        .endObject()
                        .startObject("playlist")
                            .field("type", "text")
                        .endObject()
                        .startObject("secret_soundtrack")
                            .field("type", "alias")
                            .field("path", "playlist")
                        .endObject()
                        .startObject("old_field")
                            .field("type", "long")
                        .endObject()
                        .startObject("new_field")
                            .field("type", "alias")
                            .field("path", "old_field")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        assertAcked(prepareCreate("old_index").setMapping(oldIndexMapping));

        XContentBuilder newIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("distance")
                            .field("type", "text")
                        .endObject()
                        .startObject("route_length_miles")
                            .field("type", "double")
                        .endObject()
                        .startObject("new_field")
                            .field("type", "long")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        assertAcked(prepareCreate("new_index").setMapping(newIndexMapping));
        assertAcked(client().admin().indices().prepareAliases().addAlias("new_index", "current"));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestMapperPlugin.class, ExceptionOnRewriteQueryPlugin.class);
    }

    public void testFieldAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "route_length_miles").get();

        assertIndices(response, "old_index", "new_index");
        // Ensure the response has entries for both requested fields.
        assertTrue(response.get().containsKey("distance"));
        assertTrue(response.get().containsKey("route_length_miles"));

        // Check the capabilities for the 'distance' field.
        Map<String, FieldCapabilities> distance = response.getField("distance");
        assertEquals(2, distance.size());

        assertTrue(distance.containsKey("double"));
        assertEquals(
            new FieldCapabilities("distance", "double", false, true, true, new String[] {"old_index"}, null, null,
                    Collections.emptyMap()),
            distance.get("double"));

        assertTrue(distance.containsKey("text"));
        assertEquals(
            new FieldCapabilities("distance", "text", false, true, false, new String[] {"new_index"}, null, null,
                    Collections.emptyMap()),
            distance.get("text"));

        // Check the capabilities for the 'route_length_miles' alias.
        Map<String, FieldCapabilities> routeLength = response.getField("route_length_miles");
        assertEquals(1, routeLength.size());

        assertTrue(routeLength.containsKey("double"));
        assertEquals(
            new FieldCapabilities("route_length_miles", "double", false, true, true, null, null, null, Collections.emptyMap()),
            routeLength.get("double"));
    }

    public void testFieldAliasWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("route*").get();

        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFiltering() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("secret-soundtrack", "route_length_miles").get();
        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFilteringWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "secret*").get();
        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("distance"));
    }

    public void testWithUnmapped() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps()
            .setFields("new_field", "old_field")
            .setIncludeUnmapped(true)
            .get();
        assertIndices(response, "old_index", "new_index");

        assertEquals(2, response.get().size());
        assertTrue(response.get().containsKey("old_field"));

        Map<String, FieldCapabilities> oldField = response.getField("old_field");
        assertEquals(2, oldField.size());

        assertTrue(oldField.containsKey("long"));
        assertEquals(
            new FieldCapabilities("old_field", "long", false, true, true, new String[] {"old_index"}, null, null,
                    Collections.emptyMap()),
            oldField.get("long"));

        assertTrue(oldField.containsKey("unmapped"));
        assertEquals(
            new FieldCapabilities("old_field", "unmapped", false, false, false, new String[] {"new_index"}, null, null,
                    Collections.emptyMap()),
            oldField.get("unmapped"));

        Map<String, FieldCapabilities> newField = response.getField("new_field");
        assertEquals(1, newField.size());

        assertTrue(newField.containsKey("long"));
        assertEquals(
            new FieldCapabilities("new_field", "long", false, true, true, null, null, null, Collections.emptyMap()),
            newField.get("long"));
    }

    public void testWithIndexAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps("current").setFields("*").get();
        assertIndices(response, "new_index");

        FieldCapabilitiesResponse response1 = client().prepareFieldCaps("current", "old_index").setFields("*").get();
        assertIndices(response1, "old_index", "new_index");
        FieldCapabilitiesResponse response2 = client().prepareFieldCaps("current", "old_index", "new_index").setFields("*").get();
        assertEquals(response1, response2);
    }

    public void testWithIndexFilter() throws InterruptedException {
        assertAcked(prepareCreate("index-1").setMapping("timestamp", "type=date", "field1", "type=keyword"));
        assertAcked(prepareCreate("index-2").setMapping("timestamp", "type=date", "field1", "type=long"));

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("index-1").setSource("timestamp", "2015-07-08"));
        reqs.add(client().prepareIndex("index-1").setSource("timestamp", "2018-07-08"));
        reqs.add(client().prepareIndex("index-2").setSource("timestamp", "2019-10-12"));
        reqs.add(client().prepareIndex("index-2").setSource("timestamp", "2020-07-08"));
        indexRandom(true, reqs);

        FieldCapabilitiesResponse response = client().prepareFieldCaps("index-*").setFields("*").get();
        assertIndices(response, "index-1", "index-2");
        Map<String, FieldCapabilities> newField = response.getField("field1");
        assertEquals(2, newField.size());
        assertTrue(newField.containsKey("long"));
        assertTrue(newField.containsKey("keyword"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").gte("2019-11-01"))
            .get();
        assertIndices(response, "index-2");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("long"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").lte("2017-01-01"))
            .get();
        assertIndices(response, "index-1");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("keyword"));
    }

    public void testMetadataFields() {
        for (int i = 0; i < 2; i++) {
            String[] fields = i == 0 ? new String[] { "*" } : new String[] { "_id", "_test" };
            FieldCapabilitiesResponse response = client()
                .prepareFieldCaps()
                .setFields(fields)
                .get();

            Map<String, FieldCapabilities> idField = response.getField("_id");
            assertEquals(1, idField.size());

            assertTrue(idField.containsKey("_id"));
            assertEquals(
                new FieldCapabilities("_id", "_id", true, true, false, null, null, null, Collections.emptyMap()),
                idField.get("_id"));

            Map<String, FieldCapabilities> testField = response.getField("_test");
            assertEquals(1, testField.size());

            assertTrue(testField.containsKey("keyword"));
            assertEquals(
                new FieldCapabilities("_test", "keyword", true, true, false, null, null, null, Collections.emptyMap()),
                testField.get("keyword"));
        }
    }

    public void testWithRunntimeMappings() throws InterruptedException {
        Map<String, Object> runtimeFields = new HashMap<>();
        runtimeFields.put("day_of_week", Collections.singletonMap("type", "keyword"));
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setRuntimeFields(runtimeFields).get();
        Map<String, FieldCapabilities> runtimeField = response.getField("day_of_week");
        assertNotNull(runtimeField);
        assertEquals("day_of_week", runtimeField.get("keyword").getName());
        assertEquals("keyword", runtimeField.get("keyword").getType());
        assertTrue(runtimeField.get("keyword").isSearchable());
        assertTrue(runtimeField.get("keyword").isAggregatable());
    }

    public void testFailures() throws InterruptedException {
        // in addition to the existing "old_index" and "new_index", create two where the test query throws an error on rewrite
        assertAcked(prepareCreate("index1-error"));
        assertAcked(prepareCreate("index2-error"));
        ensureGreen("index1-error", "index2-error");
        FieldCapabilitiesResponse response = client().prepareFieldCaps()
            .setFields("*")
            .setIndexFilter(new ExceptionOnRewriteQueryBuilder())
            .get();
        assertEquals(1, response.getFailures().size());
        assertEquals(2, response.getFailedIndices().length);
        assertThat(response.getFailures().get(0).getIndices(), arrayContainingInAnyOrder("index1-error", "index2-error"));
        Exception failure = response.getFailures().get(0).getException();
        assertEquals(RemoteTransportException.class, failure.getClass());
        assertEquals(IllegalArgumentException.class, failure.getCause().getClass());
        assertEquals("I throw because I choose to.", failure.getCause().getMessage());

        // the "indices" section should not include failed ones
        assertThat(Arrays.asList(response.getIndices()), containsInAnyOrder("old_index", "new_index"));

        // if all requested indices failed, we fail the request by throwing the exception
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareFieldCaps("index1-error", "index2-error")
            .setFields("*")
            .setIndexFilter(new ExceptionOnRewriteQueryBuilder())
            .get());
        assertEquals("I throw because I choose to.", ex.getMessage());
    }

    private void assertIndices(FieldCapabilitiesResponse response, String... indices) {
        assertNotNull(response.getIndices());
        Arrays.sort(indices);
        Arrays.sort(response.getIndices());
        assertArrayEquals(indices, response.getIndices());
    }

    /**
     * Adds an "exception" query that  throws on rewrite if the index name contains the string "error"
     */
    public static class ExceptionOnRewriteQueryPlugin extends Plugin implements SearchPlugin {

        public ExceptionOnRewriteQueryPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            return singletonList(
                new QuerySpec<>("exception", ExceptionOnRewriteQueryBuilder::new, p -> new ExceptionOnRewriteQueryBuilder())
            );
        }
    }

    static class ExceptionOnRewriteQueryBuilder extends AbstractQueryBuilder<ExceptionOnRewriteQueryBuilder> {

        public static final String NAME = "exception";

        ExceptionOnRewriteQueryBuilder() {}

        ExceptionOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            SearchExecutionContext searchExecutionContext = queryRewriteContext.convertToSearchExecutionContext();
            if (searchExecutionContext != null) {
                if (searchExecutionContext.indexMatches("*error*")) {
                    throw new IllegalArgumentException("I throw because I choose to.");
                };
            }
            return this;
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.endObject();
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        protected boolean doEquals(ExceptionOnRewriteQueryBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static final class TestMapperPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap(TestMetadataMapper.CONTENT_TYPE, TestMetadataMapper.PARSER);
        }

        @Override
        public Function<String, Predicate<String>> getFieldFilter() {
            return index -> field -> field.equals("playlist") == false;
        }
    }

    private static final class TestMetadataFieldType extends MappedFieldType {

        TestMetadataFieldType(String name) {
            super(name, true, false, true, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMetadataField() {
            return true;
        }

        @Override
        public String typeName() {
            return "keyword";
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new UnsupportedOperationException();
        }

    }

    private static final class TestMetadataMapper extends MetadataFieldMapper {
        private static final String CONTENT_TYPE = "_test";
        private static final String FIELD_NAME = "_test";

        protected TestMetadataMapper() {
            super(new TestMetadataFieldType(FIELD_NAME));
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) {}

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }

        private static final TypeParser PARSER = new FixedTypeParser(c -> new TestMetadataMapper());
    }
}
