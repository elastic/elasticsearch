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
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

public class RrfIT extends AbstractEsqlIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    @Before
    public void setupIndex() {
        assumeTrue("requires RRF capability", EsqlCapabilities.Cap.RRF.isEnabled());
        createAndPopulateIndex();
    }

    public void testRrf() {
        var query = """
            FROM test METADATA _score, _id, _index
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | SORT _score, _id DESC )
               ( WHERE content:"dog" | SORT _score, _id DESC )
            | RRF
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
