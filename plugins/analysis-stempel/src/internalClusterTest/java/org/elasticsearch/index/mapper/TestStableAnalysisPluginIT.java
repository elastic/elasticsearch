/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.plugin.analysis.stempel.AnalysisStempelPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class TestStableAnalysisPluginIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnalysisStempelPlugin.class);
    }

    /*
     * Example tracking tokens with case-insensitivity
     */
    public void testBasicUsage() {
        String index = "foo";

        class AnalysisTestcases {
            private final String[] phrases;
            private final AnalyzeAction.AnalyzeToken[] tokens;

            AnalysisTestcases(
                String[] phrases,
                AnalyzeAction.AnalyzeToken[] tokens) {
                this.phrases = phrases;
                this.tokens = tokens;
            }
        }

        AnalysisTestcases[] testCases = new AnalysisTestcases[]{
            new AnalysisTestcases(
                new String[]{"I like to use elastic products", "I like to use elastic products"},
                new AnalyzeAction.AnalyzeToken[] {
                    new AnalyzeAction.AnalyzeToken(
                        "ELASTIC",
                        4,
                        14,
                        14 + "elastic".length(),
                        1,
                        "<ALPHANUM>",
                        null),
                    new AnalyzeAction.AnalyzeToken(
                        "ELASTIC",
                        110,
                        45,
                        45 + "elastic".length(),
                        1,
                        "<ALPHANUM>",
                        null)
                }
            ),
            new AnalysisTestcases(
                new String[]{"I like using Elastic products", "I like using Elastic products"},
                new AnalyzeAction.AnalyzeToken[] {
                    new AnalyzeAction.AnalyzeToken(
                        "ELASTIC",
                        3,
                        13,
                        13 + "Elastic".length(),
                        1,
                        "<ALPHANUM>",
                        null),
                    new AnalyzeAction.AnalyzeToken(
                        "ELASTIC",
                        108,
                        43,
                        43 + "elastic".length(),
                        1,
                        "<ALPHANUM>",
                        null)
                }
            )};

        assertAcked(client().admin().indices().prepareCreate(index));

        for (String filter : List.of("demo_legacy", "demo")) {
            for (AnalysisTestcases testcase : testCases) {
                AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request(index).tokenizer("standard")
                    .addTokenFilter("lowercase")
                    .addTokenFilter(filter)
                    .addTokenFilter("uppercase")
                    .text(testcase.phrases);

                AnalyzeAction.Response result = client().admin().indices().analyze(analyzeRequest).actionGet();

                assertFalse(result.getTokens().isEmpty());
                assertEquals(2, result.getTokens().size());

                for (int i = 0; i < result.getTokens().size(); i++) {
                    assertThat(testcase.tokens[i], equalTo(result.getTokens().get(i)));
                }
            }
        }
    }
}
