/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.SizedParsedObject;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.TestQueryVectorBuilderPlugin;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests which parsed queries and kNN searches report {@link SizedParsedObject#UNKNOWN}, so the request keeps the estimate for their tokens.
 * These are the ones that hold parts that are not sized, and subclasses, which may keep more than the class they extend.
 * {@link ParsedContentChargeCalibrationTests} checks how close the sizes are to the heap.
 */
public class ParsedQuerySizeTests extends ESTestCase {

    private static final String QUERY_VECTOR_BUILDER =
        "\"query_vector_builder\":{\"test_query_vector_builder\":{\"query_vector\":[1.0,2.0]}}";

    private NamedXContentRegistry registry;

    @Before
    public void createRegistry() {
        registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, List.of(new TestQueryVectorBuilderPlugin())).getNamedXContents()
        );
    }

    public void testTerm() throws IOException {
        assertSized(query("{\"term\":{\"k\":\"v\"}}"));
        assertNotSized(new TermQueryBuilder("k", "v") {});
    }

    public void testTerms() throws IOException {
        assertSized(query("{\"terms\":{\"k\":[\"a\",\"b\"]}}"));
        // A lookup fetches the values later.
        assertNotSized(query("{\"terms\":{\"k\":{\"index\":\"i\",\"id\":\"1\",\"path\":\"p\"}}}"));
        assertNotSized(new TermsQueryBuilder("k", "v") {});
    }

    public void testIds() throws IOException {
        assertSized(query("{\"ids\":{\"values\":[\"a\"]}}"));
        assertNotSized(new IdsQueryBuilder() {});
    }

    public void testTermsSet() throws IOException {
        assertSized(query("{\"terms_set\":{\"k\":{\"terms\":[\"a\"],\"minimum_should_match_field\":\"n\"}}}"));
        assertNotSized(query("{\"terms_set\":{\"k\":{\"terms\":[\"a\"],\"minimum_should_match_script\":{\"source\":\"1\"}}}}"));
    }

    public void testWrapper() throws IOException {
        String source = Base64.getEncoder().encodeToString("{\"term\":{\"k\":\"v\"}}".getBytes(StandardCharsets.UTF_8));
        assertSized(query("{\"wrapper\":{\"query\":\"" + source + "\"}}"));
        assertNotSized(new WrapperQueryBuilder("{\"term\":{\"k\":\"v\"}}") {});
    }

    public void testBool() throws IOException {
        assertSized(query("{\"bool\":{\"should\":[{\"term\":{\"k\":\"v\"}}]}}"));
        assertNotSized(new BoolQueryBuilder() {});
    }

    public void testKnnQuery() throws IOException {
        assertSized(query("{\"knn\":{\"field\":\"v\",\"query_vector\":[1.0,2.0],\"num_candidates\":10}}"));
        assertNotSized(query("{\"knn\":{\"field\":\"v\"," + QUERY_VECTOR_BUILDER + ",\"num_candidates\":10}}"));
        assertNotSized(new KnnVectorQueryBuilder("v", new float[] { 1, 2 }, null, 10, null, null, null) {});
    }

    public void testKnnSearch() throws IOException {
        assertSized(knnSearch("{\"field\":\"v\",\"query_vector\":[1.0,2.0],\"k\":5,\"num_candidates\":10}"));
        assertNotSized(knnSearch("{\"field\":\"v\"," + QUERY_VECTOR_BUILDER + ",\"k\":5,\"num_candidates\":10}"));
        assertNotSized(knnSearch("{\"field\":\"v\",\"query_vector\":[1.0,2.0],\"k\":5,\"num_candidates\":10,\"inner_hits\":{}}"));
        // Without a vector, the search fails when it is built, which comes after its size is asked for.
        assertNotSized(knnSearch("{\"field\":\"v\",\"k\":5,\"num_candidates\":10}"));
        assertNotSized(
            new KnnSearchBuilder.Builder() {}.field("v").queryVector(VectorData.fromFloats(new float[] { 1, 2 })).k(5).numCandidates(10)
        );
    }

    private static void assertSized(Object parsed) {
        assertThat(parsed.toString(), ((SizedParsedObject) parsed).parsedRetainedBytes(), greaterThan(0L));
    }

    private static void assertNotSized(Object parsed) {
        assertThat(parsed.toString(), ((SizedParsedObject) parsed).parsedRetainedBytes(), equalTo(SizedParsedObject.UNKNOWN));
    }

    private QueryBuilder query(String json) throws IOException {
        try (XContentParser parser = parser(json)) {
            return AbstractQueryBuilder.parseTopLevelQuery(parser);
        }
    }

    private KnnSearchBuilder.Builder knnSearch(String json) throws IOException {
        try (XContentParser parser = parser(json)) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            return KnnSearchBuilder.fromXContent(parser);
        }
    }

    private XContentParser parser(String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY.withRegistry(registry), json);
    }
}
