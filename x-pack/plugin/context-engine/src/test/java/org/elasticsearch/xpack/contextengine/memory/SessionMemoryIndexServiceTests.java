/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class SessionMemoryIndexServiceTests extends ESTestCase {

    public void testKnnQuery() throws IOException {
        WrapperQueryBuilder query = (WrapperQueryBuilder) SessionMemoryIndexService.knnQuery(
            "dark mode",
            "test-embedding",
            20,
            List.of(QueryBuilders.termQuery("user_id", "user-1"))
        );

        Map<?, ?> knn = (Map<?, ?>) parse(query.source()).get("knn");
        assertThat(knn.get("field"), equalTo("inputs"));
        assertThat(knn.get("k"), equalTo(20));
        assertThat(knn.get("num_candidates"), equalTo(200));
        assertThat(((Map<?, ?>) ((Map<?, ?>) knn.get("query_vector_builder")).get("embedding")).get("input"), equalTo("dark mode"));
        assertThat((List<?>) knn.get("filter"), hasSize(1));
    }

    public void testKnnQueryImageInputWithoutFilters() throws IOException {
        Map<String, Object> input = Map.of("type", "image", "value", "data:image/png;base64,iVBORw0KGgo=");
        WrapperQueryBuilder query = (WrapperQueryBuilder) SessionMemoryIndexService.knnQuery(input, "test-embedding", 5, List.of());

        Map<?, ?> knn = (Map<?, ?>) parse(query.source()).get("knn");
        assertThat(knn.get("num_candidates"), equalTo(100));
        assertThat(((Map<?, ?>) ((Map<?, ?>) knn.get("query_vector_builder")).get("embedding")).get("input"), equalTo(input));
        assertThat(knn.get("filter"), nullValue());
    }

    private static Map<String, Object> parse(byte[] json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }
}
