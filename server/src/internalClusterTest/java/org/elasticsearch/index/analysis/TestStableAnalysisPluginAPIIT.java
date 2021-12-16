/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class TestStableAnalysisPluginAPIIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            DemoAnalysisPlugin.class
        );
    }

    private class AnalysisTestcases {
        private final String[] phrases;
        private final AnalyzeAction.AnalyzeToken[] tokens;

        AnalysisTestcases(
            String[] phrases,
            AnalyzeAction.AnalyzeToken[] tokens) {
            this.phrases = phrases;
            this.tokens = tokens;
        }
    }

    private AnalysisTestcases[] testCases = new AnalysisTestcases[]{
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

    public void testBasicUsage() {
        String index = "foo";

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

    public void testDetailsUsage() {
        String index = "foo";

        assertAcked(client().admin().indices().prepareCreate(index));

        for (String filter : List.of("demo_legacy", "demo")) {
            for (AnalysisTestcases testcase : testCases) {
                AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request(index).tokenizer("standard")
                    .addTokenFilter("lowercase")
                    .addTokenFilter("uppercase")
                    .addTokenFilter("lowercase")
                    .addTokenFilter(filter)
                    .addTokenFilter("uppercase")
                    .addTokenFilter("lowercase")
                    .addTokenFilter("uppercase")
                    .explain(true)
                    .text(testcase.phrases);

                AnalyzeAction.Response result = client().admin().indices().analyze(analyzeRequest).actionGet();

                assertNull(result.getTokens());
                assertEquals("standard", result.detail().tokenizer().getName());

                int numTokens = Arrays.stream(testcase.phrases).map(p -> p.split(" ").length).reduce(0, Integer::sum).intValue();
                assertEquals(numTokens, result.detail().tokenizer().getTokens().length);
                assertEquals(7, result.detail().tokenfilters().length);
                assertEquals(numTokens, result.detail().tokenfilters()[0].getTokens().length);

                int firstShrunkTokenIndex = 3;

                assertEquals(2, result.detail().tokenfilters()[firstShrunkTokenIndex].getTokens().length);

                AnalyzeAction.AnalyzeToken[] finalTokens = result.detail().tokenfilters()[firstShrunkTokenIndex+1].getTokens();
                assertEquals(2, finalTokens.length);

                for (int i = 0; i < finalTokens.length; i++) {
                    assertEquals(3, finalTokens[i].getAttributes().size());

                    assertEquals(testcase.tokens[i].getPositionLength(), finalTokens[i].getAttributes().get("positionLength"));
                    assertEquals(1, finalTokens[i].getAttributes().get("termFrequency"));

                    assertEquals(testcase.tokens[i].getTerm(), finalTokens[i].getTerm());
                    assertEquals(testcase.tokens[i].getStartOffset(), finalTokens[i].getStartOffset());
                    assertEquals(testcase.tokens[i].getEndOffset(), finalTokens[i].getEndOffset());
                    assertEquals(testcase.tokens[i].getPosition(), finalTokens[i].getPosition());
                    assertEquals(testcase.tokens[i].getPositionLength(), finalTokens[i].getPositionLength());
                }
            }
        }
    }

    public void testBasicUsageNormalizers() {
        String index = "foo";

        assertAcked(client().admin().indices().prepareCreate(index));

        for (String filter : List.of("demo_legacy", "demo")) {
            for (AnalysisTestcases testcase : testCases) {
                AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request(index)
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
