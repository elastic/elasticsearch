/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TokenListCategorizerTests extends CategorizationTestCase {

    private AnalysisRegistry analysisRegistry;

    public static AnalysisRegistry buildTestAnalysisRegistry(Environment environment) throws Exception {
        CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin();
        MachineLearning ml = new MachineLearning(environment.settings());
        return new AnalysisModule(environment, List.of(commonAnalysisPlugin, ml), new StablePluginsRegistry()).getAnalysisRegistry();
    }

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = buildTestAnalysisRegistry(environment);
    }

    public void testMinMatchingWeights() {
        assertThat(TokenListCategorizer.minMatchingWeight(0, 0.7f), equalTo(0));
        assertThat(TokenListCategorizer.minMatchingWeight(1, 0.7f), equalTo(1));
        assertThat(TokenListCategorizer.minMatchingWeight(2, 0.7f), equalTo(2));
        assertThat(TokenListCategorizer.minMatchingWeight(3, 0.7f), equalTo(3));
        assertThat(TokenListCategorizer.minMatchingWeight(4, 0.7f), equalTo(3));
        assertThat(TokenListCategorizer.minMatchingWeight(5, 0.7f), equalTo(4));
        assertThat(TokenListCategorizer.minMatchingWeight(6, 0.7f), equalTo(5));
        assertThat(TokenListCategorizer.minMatchingWeight(7, 0.7f), equalTo(5));
        assertThat(TokenListCategorizer.minMatchingWeight(8, 0.7f), equalTo(6));
        assertThat(TokenListCategorizer.minMatchingWeight(9, 0.7f), equalTo(7));
        assertThat(TokenListCategorizer.minMatchingWeight(10, 0.7f), equalTo(8));
    }

    public void testMaxMatchingWeights() {
        assertThat(TokenListCategorizer.maxMatchingWeight(0, 0.7f), equalTo(0));
        assertThat(TokenListCategorizer.maxMatchingWeight(1, 0.7f), equalTo(1));
        assertThat(TokenListCategorizer.maxMatchingWeight(2, 0.7f), equalTo(2));
        assertThat(TokenListCategorizer.maxMatchingWeight(3, 0.7f), equalTo(4));
        assertThat(TokenListCategorizer.maxMatchingWeight(4, 0.7f), equalTo(5));
        assertThat(TokenListCategorizer.maxMatchingWeight(5, 0.7f), equalTo(7));
        assertThat(TokenListCategorizer.maxMatchingWeight(6, 0.7f), equalTo(8));
        assertThat(TokenListCategorizer.maxMatchingWeight(7, 0.7f), equalTo(9));
        assertThat(TokenListCategorizer.maxMatchingWeight(8, 0.7f), equalTo(11));
        assertThat(TokenListCategorizer.maxMatchingWeight(9, 0.7f), equalTo(12));
        assertThat(TokenListCategorizer.maxMatchingWeight(10, 0.7f), equalTo(14));
    }

    public void testWeightCalculator() throws IOException {

        TokenListCategorizer.WeightCalculator weightCalculator = new TokenListCategorizer.WeightCalculator(
            CategorizationPartOfSpeechDictionary.getInstance()
        );

        assertThat(weightCalculator.calculateWeight("Info"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("my_host"), equalTo(1)); // not dictionary word
        assertThat(weightCalculator.calculateWeight("web"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("service"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("starting"), equalTo(31)); // dictionary word, verb, 3rd in a row
        assertThat(weightCalculator.calculateWeight("user123"), equalTo(1)); // not dictionary word
        assertThat(weightCalculator.calculateWeight("a"), equalTo(1)); // too short for dictionary weighting
        assertThat(weightCalculator.calculateWeight("cool"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("web"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("service"), equalTo(13)); // dictionary word, not verb, 3rd in a row
        assertThat(weightCalculator.calculateWeight("called"), equalTo(13)); // dictionary word, not verb, 4th in a row
        assertThat(weightCalculator.calculateWeight("my_service"), equalTo(1)); // not dictionary word
        assertThat(weightCalculator.calculateWeight("is"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("starting"), equalTo(6)); // dictionary word, verb, not 3rd in a row

        assertThat(TokenListCategorizer.WeightCalculator.getMinMatchingWeight(1), equalTo(1));
        assertThat(TokenListCategorizer.WeightCalculator.getMaxMatchingWeight(1), equalTo(1));
        assertThat(TokenListCategorizer.WeightCalculator.getMinMatchingWeight(3), equalTo(3));
        assertThat(TokenListCategorizer.WeightCalculator.getMaxMatchingWeight(3), equalTo(13));
        assertThat(TokenListCategorizer.WeightCalculator.getMinMatchingWeight(6), equalTo(6));
        assertThat(TokenListCategorizer.WeightCalculator.getMaxMatchingWeight(6), equalTo(31));
        assertThat(TokenListCategorizer.WeightCalculator.getMinMatchingWeight(13), equalTo(3));
        assertThat(TokenListCategorizer.WeightCalculator.getMaxMatchingWeight(13), equalTo(13));
        assertThat(TokenListCategorizer.WeightCalculator.getMinMatchingWeight(31), equalTo(6));
        assertThat(TokenListCategorizer.WeightCalculator.getMaxMatchingWeight(31), equalTo(31));
    }

    public void testApacheData() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, defaultConfig)) {

            TokenListCategorizer categorizer = new TokenListCategorizer(
                bytesRefHash,
                CategorizationPartOfSpeechDictionary.getInstance(),
                0.7f
            );

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol destroy"
                ).getId(),
                equalTo(0)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol init"
                ).getId(),
                equalTo(1)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol start"
                ).getId(),
                equalTo(2)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol stop"
                ).getId(),
                equalTo(3)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));
        }
    }

    public void testVmwareData() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, defaultConfig)) {

            TokenListCategorizer categorizer = new TokenListCategorizer(
                bytesRefHash,
                CategorizationPartOfSpeechDictionary.getInstance(),
                0.7f
            );

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Apr  5 14:53:44 host.acme.com "
                        + "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"
                ).getId(),
                equalTo(0)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Apr  5 14:53:44 host.acme.com "
                        + "Vpxa: [49EC0B90 verbose 'Default' opID=WFU-ddeadb59] [VpxaHalVmHostagent] 11: GuestInfo changed 'guest.disk"
                ).getId(),
                equalTo(1)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Apr  5 14:53:44 host.acme.com "
                        + "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Completed callback"
                ).getId(),
                equalTo(2)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Apr  5 14:53:44 host.acme.com "
                        + "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-35689729] [WaitForUpdatesDone] Received callback"
                ).getId(),
                equalTo(0)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Apr  5 14:53:44 host.acme.com "
                        + "Vpxa: [49EC0B90 verbose 'Default' opID=WFU-35689729] [VpxaHalVmHostagent] 15: GuestInfo changed 'guest.disk"
                ).getId(),
                equalTo(1)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));

            assertThat(
                computeCategory(
                    categorizationAnalyzer,
                    categorizer,
                    "Apr  5 14:53:44 host.acme.com "
                        + "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-35689729] [WaitForUpdatesDone] Completed callback"
                ).getId(),
                equalTo(2)
            );
            assertThat(categorizer.ramBytesUsed(), equalTo(categorizer.ramBytesUsedSlow()));
        }
    }

    TokenListCategory computeCategory(CategorizationAnalyzer categorizationAnalyzer, TokenListCategorizer categorizer, String message)
        throws IOException {
        try (TokenStream ts = categorizationAnalyzer.tokenStream("message", message)) {
            return categorizer.computeCategory(ts, message.length(), 1);
        }
    }
}
