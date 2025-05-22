/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.kql.KqlPlugin;
import org.junit.Before;
import org.junit.Ignore;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ScoreFunctionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSimpleWhereMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "brown")
            | WHERE match(content, "fox")
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.4274532794952393), List.of(6, 1.1248724460601807)));
        }
    }

    @Ignore
    public void testAlternativeWhereMatch() {
        var query = """
            FROM test METADATA _score
            | EVAL s1 = score(match(content, "brown"))
            | WHERE s1 > 0
            | WHERE match(content, "fox")
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.4274532794952393), List.of(6, 1.1248724460601807)));
        }
    }

    public void testSimpleScoreWhereMatch() {
        var query = """
            FROM test METADATA _score
            | EVAL first_score = score(match(content, "brown"))
            | WHERE match(content, "fox")
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.156558871269226, 0.2708943784236908), List.of(6, 0.9114001989364624, 0.21347221732139587))
            );
        }
    }

    public void testScorePlusWhereMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "brown")
            | WHERE match(content, "fox")
            | EVAL first_score = score(match(content, "brown"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.4274532794952393, 0.2708943784236908), List.of(6, 1.1248724460601807, 0.21347221732139587))
            );
        }
    }

    public void testScorePlusWhereKql() {
        var query = """
            FROM test METADATA _score
            | WHERE kql("brown")
            | WHERE match(content, "fox")
            | EVAL first_score = score(kql("brown"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.4274532794952393, 0.2708943784236908), List.of(6, 1.1248724460601807, 0.21347221732139587))
            );
        }
    }

    public void testScorePlusWhereQstr() {
        var query = """
            FROM test METADATA _score
            | WHERE qstr("brown")
            | WHERE match(content, "fox")
            | EVAL first_score = score(qstr("brown"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.4274532794952393, 0.2708943784236908), List.of(6, 1.1248724460601807, 0.21347221732139587))
            );
        }
    }

    public void testScorePlusWhereQstrAndMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE qstr("brown") AND match(content, "fox")
            | EVAL first_score = score(qstr("brown") AND match(content, "fox"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.4274532794952393, 1.4274532496929169), List.of(6, 1.1248724460601807, 1.1248724162578583))
            );
        }
    }

    public void testScorePlusWhereKqlAndMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE kql("brown") AND match(content, "fox")
            | EVAL first_score = score(kql("brown") AND match(content, "fox"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.4274532794952393, 1.4274532496929169), List.of(6, 1.1248724460601807, 1.1248724162578583))
            );
        }
    }

    public void testScorePlusWhereQstrORMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE qstr("brown") OR match(content, "fox")
            | EVAL first_score = score(qstr("brown") OR match(content, "fox"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(1, 1.4274532794952393, 1.4274532496929169),
                    List.of(2, 0.2708943784236908, 0.2708943784236908),
                    List.of(3, 0.2708943784236908, 0.2708943784236908),
                    List.of(4, 0.19301524758338928, 0.19301524758338928),
                    List.of(6, 1.1248724460601807, 1.1248724162578583)
                )
            );
        }
    }

    public void testSimpleScoreAlone() {
        var query = """
            FROM test METADATA _score
            | EVAL first_score = score(match(content, "brown"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(1, 0.0, 0.2708943784236908),
                    List.of(2, 0.0, 0.2708943784236908),
                    List.of(3, 0.0, 0.2708943784236908),
                    List.of(4, 0.0, 0.19301524758338928),
                    List.of(5, 0.0, 0.0),
                    List.of(6, 0.0, 0.21347221732139587)
                )
            );
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), KqlPlugin.class);
    }
}
