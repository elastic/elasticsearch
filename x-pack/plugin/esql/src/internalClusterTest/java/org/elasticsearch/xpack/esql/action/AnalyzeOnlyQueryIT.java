/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class AnalyzeOnlyQueryIT extends AbstractEsqlIntegTestCase {

    public void testAnalyzesValidQuery() {
        assertAcked(client().admin().indices().prepareCreate("analyze_only").setMapping("k", "type=keyword", "v", "type=long"));
        indexRandom(true, false, prepareIndex("analyze_only").setSource("k", "a", "v", 1L));

        try (EsqlQueryResponse response = analyze("FROM analyze_only | EVAL f1 = v + 1 | KEEP k, f1")) {
            assertThat(
                response.columns(),
                equalTo(List.of(new ColumnInfoImpl("k", "keyword", null), new ColumnInfoImpl("f1", "long", null)))
            );
            assertThat(getValuesList(response), empty());
        }
    }

    public void testReportsAnalysisErrors() {
        assertAcked(client().admin().indices().prepareCreate("analyze_only").setMapping("k", "type=keyword"));

        expectThrows(
            VerificationException.class,
            containsString("Unknown column [missing]"),
            () -> analyze("FROM analyze_only | KEEP missing").close()
        );
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [no_such_index]"),
            () -> analyze("FROM no_such_index").close()
        );
    }

    private EsqlQueryResponse analyze(String query) {
        var parser = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));
        var statement = parser.parse(query, new QueryParams(), new InferenceSettings(clusterService().state().metadata().settings()));
        return run(PreparedEsqlQueryRequest.analyzeOnly(statement, query));
    }
}
