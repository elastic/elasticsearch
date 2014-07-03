/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.bwcompat;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 0, scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0.0)
public class BasicAnalysisBackwardCompatibilityTests extends ElasticsearchBackwardsCompatIntegrationTest {

    /**
     * Simple upgrade test for analyzers to make sure they analyze to the same tokens after upgrade
     * TODO we need this for random tokenizers / tokenfilters as well
     */
    @Test
    public void testAnalyzerTokensAfterUpgrade() throws IOException, ExecutionException, InterruptedException {
        int numFields = randomIntBetween(PreBuiltAnalyzers.values().length, PreBuiltAnalyzers.values().length * 10);
        StringBuilder builder = new StringBuilder();
        String[] fields = new String[numFields * 2];
        int fieldId = 0;
        for (int i = 0; i < fields.length; i++) {
            fields[i++] = "field_" + fieldId++;
            String analyzer = randomAnalyzer();
            fields[i] = "type=string,analyzer=" + analyzer;
        }
        assertAcked(prepareCreate("test")
                .addMapping("type", fields)
                .setSettings(indexSettings()));
        ensureYellow();
        InputOutput[] inout = new InputOutput[numFields];
        for (int i = 0; i < numFields; i++) {
            String input = randomRealisticUnicodeOfCodepointLengthBetween(1, 100);
            AnalyzeResponse test = client().admin().indices().prepareAnalyze("test", input).setField("field_" + i).get();
            inout[i] = new InputOutput(test, input, "field_" + i);
        }

        logClusterState();
        boolean upgraded;
        do {
            logClusterState();
            upgraded = backwardsCluster().upgradeOneNode();
            ensureYellow();
        } while (upgraded);

        for (int i = 0; i < inout.length; i++) {
            InputOutput inputOutput = inout[i];
            AnalyzeResponse test = client().admin().indices().prepareAnalyze("test", inputOutput.input).setField(inputOutput.field).get();
            List<AnalyzeResponse.AnalyzeToken> tokens = test.getTokens();
            List<AnalyzeResponse.AnalyzeToken>  expectedTokens = inputOutput.response.getTokens();
            assertThat("size mismatch field: " + fields[i*2] + " analyzer: " + fields[i*2 + 1] + " input: " + inputOutput.input, expectedTokens.size(), equalTo(tokens.size()));
            for (int j = 0; j < tokens.size(); j++) {
                String msg = "failed for term: " + expectedTokens.get(j).getTerm() + " field: " + fields[i*2] + " analyzer: " + fields[i*2 + 1] + " input: " + inputOutput.input;
                assertThat(msg, expectedTokens.get(j).getTerm(), equalTo(tokens.get(j).getTerm()));
                assertThat(msg, expectedTokens.get(j).getPosition(), equalTo(tokens.get(j).getPosition()));
                assertThat(msg, expectedTokens.get(j).getStartOffset(), equalTo(tokens.get(j).getStartOffset()));
                assertThat(msg, expectedTokens.get(j).getEndOffset(), equalTo(tokens.get(j).getEndOffset()));
                assertThat(msg, expectedTokens.get(j).getType(), equalTo(tokens.get(j).getType()));
            }
        }
    }

    private String randomAnalyzer() {
        while(true) {
            PreBuiltAnalyzers preBuiltAnalyzers = RandomPicks.randomFrom(getRandom(), PreBuiltAnalyzers.values());
            if (preBuiltAnalyzers == PreBuiltAnalyzers.SORANI && compatibilityVersion().before(Version.V_1_3_0)) {
                continue; // SORANI was added in 1.3.0
            }
            return preBuiltAnalyzers.name().toLowerCase(Locale.ROOT);
        }

    }

    private static final class InputOutput {
        final AnalyzeResponse response;
        final String input;
        final String field;

        public InputOutput(AnalyzeResponse response, String input, String field) {
            this.response = response;
            this.input = input;
            this.field = field;
        }


    }
}
