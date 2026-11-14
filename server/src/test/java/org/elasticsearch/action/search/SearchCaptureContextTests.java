/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.mapper.vectors.DecodedVector;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class SearchCaptureContextTests extends ESTestCase {

    private static final float[] VECTOR = new float[] { 0.1f, -1.5f, 3.25f, 42.125f };

    private static SearchCaptureContext captureContext(SearchSourceBuilder source) {
        var task = new Task(randomNonNegativeLong(), "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of());
        var request = new SearchRequest().source(source);
        return new SearchCaptureContext(task, new NamedWriteableRegistry(List.of()), request, 1L, new RuntimeException("test"));
    }

    /** Asserts the logged query holds the base64 form of the vector and not the numeric array form. */
    private static void assertBase64Vector(String query) {
        assertThat(query, containsString("\"" + DecodedVector.floatsToBase64(VECTOR) + "\""));
        assertThat(query, not(containsString("42.125")));
    }

    public void testTopLevelKnnQueryVectorIsBase64Encoded() {
        var source = new SearchSourceBuilder().knnSearch(List.of(new KnnSearchBuilder("field", VECTOR, 5, 10, null, null, null)));
        assertBase64Vector(captureContext(source).getQuery());
    }

    public void testKnnQueryVectorIsBase64Encoded() {
        var source = new SearchSourceBuilder().query(new KnnVectorQueryBuilder("field", VECTOR, 5, 10, null, null, null));
        assertBase64Vector(captureContext(source).getQuery());
    }

    public void testKnnRetrieverQueryVectorIsBase64Encoded() {
        var source = new SearchSourceBuilder().retriever(new KnnRetrieverBuilder("field", VECTOR, null, 5, 10, null, null, null));
        assertBase64Vector(captureContext(source).getQuery());
    }

    public void testKnnInFilterAggregationIsBase64Encoded() {
        var knn = new KnnVectorQueryBuilder("field", VECTOR, 5, 10, null, null, null);
        var source = new SearchSourceBuilder().aggregation(AggregationBuilders.filter("agg", knn));
        assertBase64Vector(captureContext(source).getQuery());
    }

    public void testNonVectorQueryIsUnaffected() {
        var source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        assertThat(captureContext(source).getQuery(), containsString("match_all"));
    }

    public void testByteVectorIsStillHexEncoded() {
        var vector = VectorData.fromBytes(new byte[] { 1, 2, 3 });
        var source = new SearchSourceBuilder().knnSearch(List.of(new KnnSearchBuilder("field", vector, 5, 10, null, null, null)));
        assertThat(captureContext(source).getQuery(), containsString("\"010203\""));
    }
}
