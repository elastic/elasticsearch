/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Integration tests for knn search on vectors in nested fields.
 */
public class NestedKnnIT extends ESIntegTestCase {

    /**
     * Checks that a filtered alias restricts the knn candidates on a vector nested one, two, and three levels deep.
     */
    public void testKnnThroughFilteredAlias() throws Exception {
        CheckedConsumer<Integer, Exception> testDepth = depth -> {
            String index = "test_" + depth;
            String nameAlias = index + "_name_alias";
            String tagAlias = index + "_tag_alias";
            assertAcked(
                prepareCreate(index).setMapping(mapping(depth))
                    .addAlias(new Alias(nameAlias).filter(QueryBuilders.termQuery("name", "a")))
                    .addAlias(
                        new Alias(tagAlias).filter(
                            QueryBuilders.nestedQuery("tags", QueryBuilders.termQuery("tags.tag", "x"), ScoreMode.None)
                        )
                    )
            );
            ensureGreen(index);

            prepareIndex(index).setId("1").setSource(doc("a", "x", new float[] { 1, 1, 1 }, depth)).get(TEST_REQUEST_TIMEOUT);
            prepareIndex(index).setId("2").setSource(doc("b", "y", new float[] { 1, 1, 2 }, depth)).get(TEST_REQUEST_TIMEOUT);
            refresh(index);

            // The index returns both docs, while each alias must return only doc 1
            QueryBuilder query = knnQuery(depth, new float[] { 1, 1, 2 });
            assertSearchHitsWithoutFailures(prepareSearch(index).setQuery(query), "1", "2");
            for (String alias : List.of(nameAlias, tagAlias)) {
                assertSearchHitsWithoutFailures(prepareSearch(alias).setQuery(query), "1");
            }
        };

        for (int depth = 1; depth <= 3; depth++) {
            testDepth.accept(depth);
        }
    }

    private static XContentBuilder mapping(int depth) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().startObject("properties");
        builder.startObject("name").field("type", "keyword").endObject();
        builder.startObject("tags")
            .field("type", "nested")
            .startObject("properties")
            .startObject("tag")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        for (int i = 0; i < depth; i++) {
            builder.startObject("nested").field("type", "nested").startObject("properties");
        }
        builder.startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 3)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject();
        for (int i = 0; i < depth; i++) {
            builder.endObject().endObject();
        }
        return builder.endObject().endObject();
    }

    private static Map<String, Object> doc(String name, String tag, float[] vector, int depth) {
        Map<String, Object> nested = Map.of("vector", vector);
        for (int i = 1; i < depth; i++) {
            nested = Map.of("nested", List.of(nested));
        }
        return Map.of("name", name, "tags", List.of(Map.of("tag", tag)), "nested", List.of(nested));
    }

    private static QueryBuilder knnQuery(int depth, float[] queryVector) {
        QueryBuilder query = new KnnVectorQueryBuilder(nestedPath(depth) + ".vector", queryVector, 10, null, 10f, null, null);
        for (int i = depth; i >= 1; i--) {
            query = new NestedQueryBuilder(nestedPath(i), query, ScoreMode.Max);
        }
        return query;
    }

    private static String nestedPath(int depth) {
        return String.join(".", Collections.nCopies(depth, "nested"));
    }
}
