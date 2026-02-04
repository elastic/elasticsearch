/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

public class FuseIT extends AbstractEsqlIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testFuseWithRrf() throws Exception {
        var query = """
            FROM test METADATA _score, _id, _index
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | SORT _score, _id DESC )
               ( WHERE content:"dog" | SORT _score, _id DESC )
            | FUSE
            | SORT _score DESC, _id, _index
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 4)
            | KEEP id, content, _score, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content", "_score", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "double", "keyword"));
            assertThat(getValuesList(resp.values()).size(), equalTo(3));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(6, "The quick brown fox jumps over the lazy dog", 0.0325, List.of("fork1", "fork2")),
                List.of(4, "The dog is brown but this document is very very long", 0.0164, "fork2"),
                List.of(3, "This dog is really brown", 0.0159, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testFuseRrfWithWeights() {
        var query = """
            FROM test METADATA _score, _id, _index
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | SORT _score, _id DESC )
               ( WHERE content:"dog" | SORT _score, _id DESC )
            | FUSE RRF WITH {"weights": { "fork1": 0.4, "fork2": 0.6}}
            | SORT _score DESC, _id, _index
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 4)
            | KEEP id, content, _score, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content", "_score", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "double", "keyword"));
            assertThat(getValuesList(resp.values()).size(), equalTo(3));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(6, "The quick brown fox jumps over the lazy dog", 0.0162, List.of("fork1", "fork2")),
                List.of(4, "The dog is brown but this document is very very long", 0.0098, "fork2"),
                List.of(3, "This dog is really brown", 0.0095, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testFuseRrfWithWeightsAndRankConstant() {
        var query = """
            FROM test METADATA _score, _id, _index
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | SORT _score, _id DESC )
               ( WHERE content:"dog" | SORT _score, _id DESC )
            | FUSE RRF WITH {"weights": { "fork1": 0.4, "fork2": 0.6}, "rank_constant": 55 }
            | SORT _score DESC, _id, _index
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 4)
            | KEEP id, content, _score, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content", "_score", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "double", "keyword"));
            assertThat(getValuesList(resp.values()).size(), equalTo(3));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(6, "The quick brown fox jumps over the lazy dog", 0.0177, List.of("fork1", "fork2")),
                List.of(4, "The dog is brown but this document is very very long", 0.0107, "fork2"),
                List.of(3, "This dog is really brown", 0.0103, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testFuseSimpleLinear() {
        var query = """
            FROM test METADATA _score, _id, _index
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | SORT _score, _id DESC )
               ( WHERE content:"dog" | SORT _score, _id DESC )
            | FUSE linear
            | SORT _score DESC
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 4)
            | KEEP id, content, _score, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content", "_score", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "double", "keyword"));
            assertThat(getValuesList(resp.values()).size(), equalTo(3));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(6, "The quick brown fox jumps over the lazy dog", 1.3025, List.of("fork1", "fork2")),
                List.of(3, "This dog is really brown", 0.4963, "fork2"),
                List.of(4, "The dog is brown but this document is very very long", 0.3536, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testFuseLinearWithWeightsAndNormalizer() {
        assumeTrue("requires FUSE_L2_NORM capability", EsqlCapabilities.Cap.FUSE_L2_NORM.isEnabled());

        var query = """
            FROM test METADATA _score, _id, _index
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | SORT _score, _id DESC )
               ( WHERE content:"dog" | SORT _score, _id DESC )
            | FUSE LINEAR WITH {"weights": { "fork1": 0.4, "fork2": 0.6}, "normalizer": "l2_norm"}
            | SORT _score DESC
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 4)
            | KEEP id, content, _score, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content", "_score", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "double", "keyword"));
            assertThat(getValuesList(resp.values()).size(), equalTo(3));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(6, "The quick brown fox jumps over the lazy dog", 0.7241, List.of("fork1", "fork2")),
                List.of(3, "This dog is really brown", 0.4112, "fork2"),
                List.of(4, "The dog is brown but this document is very very long", 0.293, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testFuseWithSingleFork() {
        for (Fuse.FuseType type : Fuse.FuseType.values()) {
            var query = """
                FROM test METADATA _score, _id, _index
                | WHERE id > 2
                | FORK
                   ( WHERE content:"fox" | SORT _score, _id DESC )
                | FUSE
                """ + type.name() + """
                | SORT _score DESC, _id, _index
                | EVAL _fork = mv_sort(_fork)
                | EVAL _score = round(_score, 4)
                | KEEP id, content, _fork
                """;
            try (var resp = run(query)) {
                assertColumnNames(resp.columns(), List.of("id", "content", "_fork"));
                assertColumnTypes(resp.columns(), List.of("integer", "keyword", "keyword"));
                assertThat(getValuesList(resp.values()).size(), equalTo(1));
                Iterable<Iterable<Object>> expectedValues = List.of(List.of(6, "The quick brown fox jumps over the lazy dog", "fork1"));
                assertValues(resp.values(), expectedValues);
            }
        }
    }

    private void createAndPopulateIndex() {
        var indexName = "test";
        var client = client().admin().indices();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(CreateRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }
}
