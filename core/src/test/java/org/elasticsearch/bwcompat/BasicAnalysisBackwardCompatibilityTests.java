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
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.TestUtil;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 0, scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0.0)
public class BasicAnalysisBackwardCompatibilityTests extends ElasticsearchBackwardsCompatIntegrationTest {

    // This pattern match characters with Line_Break = Complex_Content.
    final static Pattern complexUnicodeChars = Pattern.compile("[\u17B4\u17B5\u17D3\u17CB-\u17D1\u17DD\u1036\u17C6\u1A74\u1038\u17C7\u0E4E\u0E47-\u0E4D\u0EC8-\u0ECD\uAABF\uAAC1\u1037\u17C8-\u17CA\u1A75-\u1A7C\u1AA8-\u1AAB\uAADE\uAADF\u1AA0-\u1AA6\u1AAC\u1AAD\u109E\u109F\uAA77-\uAA79\u0E46\u0EC6\u17D7\u1AA7\uA9E6\uAA70\uAADD\u19DA\u0E01-\u0E3A\u0E40-\u0E45\u0EDE\u0E81\u0E82\u0E84\u0E87\u0E88\u0EAA\u0E8A\u0EDF\u0E8D\u0E94-\u0E97\u0E99-\u0E9F\u0EA1-\u0EA3\u0EA5\u0EA7\u0EAB\u0EDC\u0EDD\u0EAD-\u0EB9\u0EBB-\u0EBD\u0EC0-\u0EC4\uAA80-\uAABE\uAAC0\uAAC2\uAADB\uAADC\u1000\u1075\u1001\u1076\u1002\u1077\uAA60\uA9E9\u1003\uA9E0\uA9EA\u1004\u105A\u1005\u1078\uAA61\u1006\uA9E1\uAA62\uAA7E\u1007\uAA63\uA9EB\u1079\uAA72\u1008\u105B\uA9E2\uAA64\uA9EC\u1061\uAA7F\u1009\u107A\uAA65\uA9E7\u100A\u100B\uAA66\u100C\uAA67\u100D\uAA68\uA9ED\u100E\uAA69\uA9EE\u100F\u106E\uA9E3\uA9EF\u1010-\u1012\u107B\uA9FB\u1013\uAA6A\uA9FC\u1014\u107C\uAA6B\u105E\u1015\u1016\u107D\u107E\uAA6F\u108E\uA9E8\u1017\u107F\uA9FD\u1018\uA9E4\uA9FE\u1019\u105F\u101A\u103B\u101B\uAA73\uAA7A\u103C\u101C\u1060\u101D\u103D\u1082\u1080\u1050\u1051\u1065\u101E\u103F\uAA6C\u101F\u1081\uAA6D\u103E\uAA6E\uAA71\u1020\uA9FA\u105C\u105D\u106F\u1070\u1066\u1021-\u1026\u1052-\u1055\u1027-\u102A\u102C\u102B\u1083\u1072\u109C\u102D\u1071\u102E\u1033\u102F\u1073\u1074\u1030\u1056-\u1059\u1031\u1084\u1035\u1085\u1032\u109D\u1034\u1062\u1067\u1068\uA9E5\u1086\u1039\u103A\u1063\u1064\u1069-\u106D\u1087\u108B\u1088\u108C\u108D\u1089\u108A\u108F\u109A\u109B\uAA7B-\uAA7D\uAA74-\uAA76\u1780-\u17A2\u17DC\u17A3-\u17B3\u17B6-\u17C5\u17D2\u1950-\u196D\u1970-\u1974\u1980-\u199C\u19DE\u19DF\u199D-\u19AB\u19B0-\u19C9\u1A20-\u1A26\u1A58\u1A59\u1A27-\u1A3B\u1A5A\u1A5B\u1A3C-\u1A46\u1A54\u1A47-\u1A4C\u1A53\u1A6B\u1A55-\u1A57\u1A5C-\u1A5E\u1A4D-\u1A52\u1A61\u1A6C\u1A62-\u1A6A\u1A6E\u1A6F\u1A73\u1A70-\u1A72\u1A6D\u1A60]");

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
            String input;
            Matcher matcher;
            do {
                // In Lucene 4.10, a bug was fixed in StandardTokenizer which was causing breaks on complex characters.
                // The bug was fixed without backcompat Version handling, so testing between >=4.10 vs <= 4.9 can
                // cause differences when the random string generated contains these complex characters. To mitigate
                // the problem, we skip any strings containing these characters.
                // TODO: only skip strings containing complex chars when comparing against ES <= 1.3.x
                input = TestUtil.randomAnalysisString(getRandom(), 100, false);
                matcher = complexUnicodeChars.matcher(input);
            } while (matcher.find());

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
            assertThat("size mismatch field: " + fields[i*2] + " analyzer: " + fields[i*2 + 1] + " input: " + BaseTokenStreamTestCase.escape(inputOutput.input), expectedTokens.size(), equalTo(tokens.size()));
            for (int j = 0; j < tokens.size(); j++) {
                String msg = "failed for term: " + expectedTokens.get(j).getTerm() + " field: " + fields[i*2] + " analyzer: " + fields[i*2 + 1] + " input: " + BaseTokenStreamTestCase.escape(inputOutput.input);
                assertThat(msg,  BaseTokenStreamTestCase.escape(expectedTokens.get(j).getTerm()), equalTo(BaseTokenStreamTestCase.escape(tokens.get(j).getTerm())));
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
