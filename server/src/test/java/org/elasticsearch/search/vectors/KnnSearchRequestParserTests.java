/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.vectors.KnnSearchRequestParser.KnnSearch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchSourceBuilder;
import static org.hamcrest.Matchers.containsString;

public class KnnSearchRequestParserTests extends ESTestCase {
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerNamedXContents() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    public void testParseSearchRequest() throws IOException {
        // Choose random REST parameters
        Map<String, String> params = new HashMap<>();
        String[] indices = generateRandomStringArray(5, 10, false, true);
        params.put(KnnSearchRequestParser.INDEX_PARAM, String.join(",", indices));

        String routing = null;
        if (randomBoolean()) {
            routing = randomAlphaOfLengthBetween(3, 10);
            params.put(KnnSearchRequestParser.ROUTING_PARAM, routing);
        }

        // Create random request body
        KnnSearch knnSearch = randomKnnSearch();
        List<QueryBuilder> filterQueries = randomFilterQueries();
        SearchSourceBuilder searchSource = randomSearchSourceBuilder(
            () -> null,
            () -> null,
            () -> null,
            () -> null,
            Collections::emptyList,
            () -> null,
            () -> null
        );
        XContentBuilder builder = createRequestBody(knnSearch, filterQueries, searchSource);

        // Convert the REST request to a search request and check the components
        SearchRequestBuilder searchRequestBuilder = parseSearchRequest(builder, params);
        SearchRequest searchRequest = searchRequestBuilder.request();

        assertArrayEquals(indices, searchRequest.indices());
        assertEquals(routing, searchRequest.routing());

        KnnVectorQueryBuilder query = knnSearch.toQueryBuilder();
        if (filterQueries.isEmpty() == false) {
            query.addFilterQueries(filterQueries);
        }
        assertEquals(query, searchRequest.source().query());
        assertEquals(knnSearch.k, searchRequest.source().size());

        assertEquals(searchSource.fetchSource(), searchRequest.source().fetchSource());
        assertEquals(searchSource.fetchFields(), searchRequest.source().fetchFields());
        assertEquals(searchSource.docValueFields(), searchRequest.source().docValueFields());
        assertEquals(searchSource.storedFields(), searchRequest.source().storedFields());
    }

    public void testParseSourceString() throws IOException {
        // Create random request body
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());

        KnnSearch knnSearch = randomKnnSearch();
        builder.startObject()
            .startObject(KnnSearchRequestParser.KNN_SECTION_FIELD.getPreferredName())
            .field(KnnSearch.FIELD_FIELD.getPreferredName(), knnSearch.field)
            .field(KnnSearch.K_FIELD.getPreferredName(), knnSearch.k)
            .field(KnnSearch.NUM_CANDS_FIELD.getPreferredName(), knnSearch.numCands)
            .field(KnnSearch.QUERY_VECTOR_FIELD.getPreferredName(), knnSearch.queryVector)
            .endObject();

        builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), "some-field");
        builder.endObject();

        // Convert the REST request to a search request and check the components
        SearchRequestBuilder searchRequestBuilder = parseSearchRequest(builder);
        SearchRequest searchRequest = searchRequestBuilder.request();

        FetchSourceContext fetchSource = searchRequest.source().fetchSource();
        assertTrue(fetchSource.fetchSource());
        assertArrayEquals(new String[] { "some-field" }, fetchSource.includes());
    }

    public void testParseSourceArray() throws IOException {
        // Create random request body
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());

        KnnSearch knnSearch = randomKnnSearch();
        builder.startObject()
            .startObject(KnnSearchRequestParser.KNN_SECTION_FIELD.getPreferredName())
            .field(KnnSearch.FIELD_FIELD.getPreferredName(), knnSearch.field)
            .field(KnnSearch.K_FIELD.getPreferredName(), knnSearch.k)
            .field(KnnSearch.NUM_CANDS_FIELD.getPreferredName(), knnSearch.numCands)
            .field(KnnSearch.QUERY_VECTOR_FIELD.getPreferredName(), knnSearch.queryVector)
            .endObject();

        builder.array(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), "field1", "field2", "field3");
        builder.endObject();

        // Convert the REST request to a search request and check the components
        SearchRequestBuilder searchRequestBuilder = parseSearchRequest(builder);
        SearchRequest searchRequest = searchRequestBuilder.request();

        FetchSourceContext fetchSource = searchRequest.source().fetchSource();
        assertTrue(fetchSource.fetchSource());
        assertArrayEquals(new String[] { "field1", "field2", "field3" }, fetchSource.includes());
    }

    public void testMissingKnnSection() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())
            .startObject()
            .array(SearchSourceBuilder.FETCH_FIELDS_FIELD.getPreferredName(), "field1", "field2")
            .endObject();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseSearchRequest(builder));
        assertThat(e.getMessage(), containsString("missing required [knn] section in search body"));
    }

    public void testNumCandsLessThanK() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())
            .startObject()
            .startObject(KnnSearchRequestParser.KNN_SECTION_FIELD.getPreferredName())
            .field(KnnSearch.FIELD_FIELD.getPreferredName(), "field")
            .field(KnnSearch.K_FIELD.getPreferredName(), 100)
            .field(KnnSearch.NUM_CANDS_FIELD.getPreferredName(), 80)
            .field(KnnSearch.QUERY_VECTOR_FIELD.getPreferredName(), new float[] { 1.0f, 2.0f, 3.0f })
            .endObject()
            .endObject();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseSearchRequest(builder));
        assertThat(e.getMessage(), containsString("[num_candidates] cannot be less than [k]"));
    }

    public void testNumCandsExceedsLimit() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())
            .startObject()
            .startObject(KnnSearchRequestParser.KNN_SECTION_FIELD.getPreferredName())
            .field(KnnSearch.FIELD_FIELD.getPreferredName(), "field")
            .field(KnnSearch.K_FIELD.getPreferredName(), 100)
            .field(KnnSearch.NUM_CANDS_FIELD.getPreferredName(), 10002)
            .field(KnnSearch.QUERY_VECTOR_FIELD.getPreferredName(), new float[] { 1.0f, 2.0f, 3.0f })
            .endObject()
            .endObject();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseSearchRequest(builder));
        assertThat(e.getMessage(), containsString("[num_candidates] cannot exceed [10000]"));
    }

    public void testInvalidK() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())
            .startObject()
            .startObject(KnnSearchRequestParser.KNN_SECTION_FIELD.getPreferredName())
            .field(KnnSearch.FIELD_FIELD.getPreferredName(), "field")
            .field(KnnSearch.K_FIELD.getPreferredName(), 0)
            .field(KnnSearch.NUM_CANDS_FIELD.getPreferredName(), 10)
            .field(KnnSearch.QUERY_VECTOR_FIELD.getPreferredName(), new float[] { 1.0f, 2.0f, 3.0f })
            .endObject()
            .endObject();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseSearchRequest(builder));
        assertThat(e.getMessage(), containsString("[k] must be greater than 0"));
    }

    private SearchRequestBuilder parseSearchRequest(XContentBuilder builder) throws IOException {
        Map<String, String> params = Map.of(KnnSearchRequestParser.INDEX_PARAM, "index");
        return parseSearchRequest(builder, params);
    }

    private SearchRequestBuilder parseSearchRequest(XContentBuilder builder, Map<String, String> params) throws IOException {
        KnnSearchRequestParser knnRequestBuilder = KnnSearchRequestParser.parseRestRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withParams(params)
                .withContent(BytesReference.bytes(builder), builder.contentType())
                .build()
        );
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(null, SearchAction.INSTANCE);
        knnRequestBuilder.toSearchRequest(searchRequestBuilder);
        return searchRequestBuilder;
    }

    private KnnSearch randomKnnSearch() {
        String field = randomAlphaOfLength(6);
        int dim = randomIntBetween(2, 30);
        float[] vector = new float[dim];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }

        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k, 1000);
        return new KnnSearch(field, vector, k, numCands);
    }

    private List<QueryBuilder> randomFilterQueries() {
        List<QueryBuilder> filters = new ArrayList<>();
        int numFilters = randomIntBetween(0, 3);
        for (int i = 0; i < numFilters; i++) {
            QueryBuilder filter = QueryBuilders.termQuery(randomAlphaOfLength(5), randomAlphaOfLength(10));
            filters.add(filter);
        }
        return filters;
    }

    private XContentBuilder createRequestBody(KnnSearch knnSearch, List<QueryBuilder> filters, SearchSourceBuilder searchSource)
        throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        builder.startObject();

        builder.startObject(KnnSearchRequestParser.KNN_SECTION_FIELD.getPreferredName())
            .field(KnnSearch.FIELD_FIELD.getPreferredName(), knnSearch.field)
            .field(KnnSearch.K_FIELD.getPreferredName(), knnSearch.k)
            .field(KnnSearch.NUM_CANDS_FIELD.getPreferredName(), knnSearch.numCands)
            .field(KnnSearch.QUERY_VECTOR_FIELD.getPreferredName(), knnSearch.queryVector)
            .endObject();

        if (filters.isEmpty() == false) {
            builder.field(KnnSearchRequestParser.FILTER_FIELD.getPreferredName());
            if (filters.size() > 1) {
                builder.startArray();
            }
            for (QueryBuilder filter : filters) {
                filter.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            if (filters.size() > 1) {
                builder.endArray();
            }
        }

        if (searchSource.fetchSource() != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName());
            searchSource.fetchSource().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        if (searchSource.fetchFields() != null) {
            builder.startArray(SearchSourceBuilder.FETCH_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat fieldAndFormat : searchSource.fetchFields()) {
                fieldAndFormat.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endArray();
        }

        if (searchSource.docValueFields() != null) {
            builder.startArray(SearchSourceBuilder.DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat fieldAndFormat : searchSource.docValueFields()) {
                fieldAndFormat.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endArray();
        }

        if (searchSource.storedFields() != null) {
            searchSource.storedFields().toXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), builder);
        }

        builder.endObject();
        return builder;
    }

}
