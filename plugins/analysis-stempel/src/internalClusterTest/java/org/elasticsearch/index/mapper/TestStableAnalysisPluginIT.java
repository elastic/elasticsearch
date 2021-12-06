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

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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
            private final String phrase;
            private final String token;
            private final int startOffset;

            AnalysisTestcases(String phrase, String token, int startOffset) {
                this.phrase = phrase;
                this.token = token;
                this.startOffset = startOffset;
            }
        }

        AnalysisTestcases[] testCases = new AnalysisTestcases[] {
            new AnalysisTestcases("I like to use elastic products", "elastic", 14),
            new AnalysisTestcases("I like using Elastic products", "Elastic", 13) };

        assertAcked(client().admin().indices().prepareCreate(index));

        for (AnalysisTestcases testcase : testCases) {
            AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request(index).tokenizer("standard")
                .addTokenFilter("nikola")
                .text(testcase.phrase);

            AnalyzeAction.Response result = client().admin().indices().analyze(analyzeRequest).actionGet();

            assertFalse(result.getTokens().isEmpty());
            assertThat(1, equalTo(result.getTokens().size()));
            AnalyzeAction.AnalyzeToken token = result.getTokens().get(0);

            assertEquals(testcase.token, token.getTerm());
            assertEquals(testcase.startOffset, token.getStartOffset());
        }

    }
}
