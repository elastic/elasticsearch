/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CategorizationAnalyzerTests extends ESTestCase {

    private AnalysisRegistry analysisRegistry;

    public static AnalysisRegistry buildTestAnalysisRegistry(Environment environment) throws Exception {
        CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin();
        MachineLearning ml = new MachineLearning(environment.settings(), environment.configFile());
        return new AnalysisModule(environment, Arrays.asList(commonAnalysisPlugin, ml)).getAnalysisRegistry();
    }

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = buildTestAnalysisRegistry(environment);
    }

    public void testVerifyConfigBuilder_GivenNoConfig() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("categorization_analyzer that is not a global analyzer must specify a [tokenizer] field", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenDefault() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder(defaultConfig);
        CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry);
    }

    public void testVerifyConfigBuilder_GivenValidAnalyzer() throws IOException {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().setAnalyzer("standard");
        CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry);
    }

    public void testVerifyConfigBuilder_GivenInvalidAnalyzer() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().setAnalyzer("does not exist");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("Failed to find global analyzer [does not exist]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenValidCustomConfig() throws IOException {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .addCharFilter(ignoreStuffInSqaureBrackets)
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter(ignoreStuffThatBeginsWithADigit)
            .addTokenFilter("snowball");
        CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry);
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithInvalidCharFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .addCharFilter("wrong!")
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("failed to find global char_filter under [wrong!]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithMisconfiguredCharFilter() {
        Map<String, Object> noPattern = new HashMap<>();
        noPattern.put("type", "pattern_replace");
        noPattern.put("attern", "should have been pattern");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .addCharFilter(noPattern)
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("pattern is missing for [__anonymous__pattern_replace] char filter of type 'pattern_replace'", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithInvalidTokenizer() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .addCharFilter(ignoreStuffInSqaureBrackets)
            .setTokenizer("oops!")
            .addTokenFilter("lowercase")
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("failed to find global tokenizer under [oops!]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenNoTokenizer() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .addCharFilter(ignoreStuffInSqaureBrackets)
            .addTokenFilter("lowercase")
            .addTokenFilter(ignoreStuffThatBeginsWithADigit)
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("categorization_analyzer that is not a global analyzer must specify a [tokenizer] field", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithInvalidTokenFilter() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .addCharFilter(ignoreStuffInSqaureBrackets)
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter("oh dear!");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("failed to find global filter under [oh dear!]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithMisconfiguredTokenFilter() {
        Map<String, Object> noPattern = new HashMap<>();
        noPattern.put("type", "pattern_replace");
        noPattern.put("attern", "should have been pattern");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .addCharFilter("html_strip")
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter(noPattern);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("pattern is missing for [__anonymous__pattern_replace] token filter of type 'pattern_replace'", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenAnalyzerAndCharFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .setAnalyzer("standard")
            .addCharFilter("html_strip");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [char_filter] field", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenAnalyzerAndTokenizer() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .setAnalyzer("standard")
            .setTokenizer("classic");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [tokenizer] field", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenAnalyzerAndTokenFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder()
            .setAnalyzer("standard")
            .addTokenFilter("lowercase");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry));
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [filter] field", e.getMessage());
    }

    // The default categorization analyzer matches what the analyzer in the ML C++ does
    public void testDefaultCategorizationAnalyzer() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, defaultConfig)) {

            assertEquals(Arrays.asList("ml13-4608.1.p2ps", "Info", "Source", "ML_SERVICE2", "on", "has", "shut", "down"),
                    categorizationAnalyzer.tokenizeField("p2ps",
                            "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down."));

            assertEquals(Arrays.asList("Vpxa", "verbose", "VpxaHalCnxHostagent", "opID", "WFU-ddeadb59", "WaitForUpdatesDone", "Received",
                    "callback"),
                    categorizationAnalyzer.tokenizeField("vmware",
                            "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"));

            assertEquals(Arrays.asList("org.apache.coyote.http11.Http11BaseProtocol", "destroy"),
                    categorizationAnalyzer.tokenizeField("apache",
                            "org.apache.coyote.http11.Http11BaseProtocol destroy"));

            assertEquals(Arrays.asList("INFO", "session", "PROXY", "Session", "DESTROYED"),
                    categorizationAnalyzer.tokenizeField("proxy",
                            " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - " +
                                    "----------------- PROXY Session DESTROYED --------------------"));

            assertEquals(Arrays.asList("PSYoungGen", "total", "used"),
                    categorizationAnalyzer.tokenizeField("java",
                            "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"));
        }
    }

    public void testDefaultCategorizationAnalyzerWithCategorizationFilter() throws IOException {
        // A categorization filter that removes stuff in square brackets
        CategorizationAnalyzerConfig defaultConfigWithCategorizationFilter =
                CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(Collections.singletonList("\\[[^\\]]*\\]"));
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry,
                defaultConfigWithCategorizationFilter)) {

            assertEquals(Arrays.asList("ml13-4608.1.p2ps", "Info", "Source", "ML_SERVICE2", "on", "has", "shut", "down"),
                    categorizationAnalyzer.tokenizeField("p2ps",
                            "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down."));

            assertEquals(Arrays.asList("Vpxa", "Received", "callback"),
                    categorizationAnalyzer.tokenizeField("vmware",
                            "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"));

            assertEquals(Arrays.asList("org.apache.coyote.http11.Http11BaseProtocol", "destroy"),
                    categorizationAnalyzer.tokenizeField("apache",
                            "org.apache.coyote.http11.Http11BaseProtocol destroy"));

            assertEquals(Arrays.asList("INFO", "session", "PROXY", "Session", "DESTROYED"),
                    categorizationAnalyzer.tokenizeField("proxy",
                            " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - " +
                                    "----------------- PROXY Session DESTROYED --------------------"));

            assertEquals(Arrays.asList("PSYoungGen", "total", "used"),
                    categorizationAnalyzer.tokenizeField("java",
                            "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"));
        }
    }

    // The Elasticsearch standard analyzer - this is the default for indexing in Elasticsearch, but
    // NOT for ML categorization (and you'll see why if you look at the expected results of this test!)
    public void testStandardAnalyzer() throws IOException {
        CategorizationAnalyzerConfig config = new CategorizationAnalyzerConfig.Builder().setAnalyzer("standard").build();
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, config)) {

            assertEquals(Arrays.asList("ml13", "4608.1", "p2ps", "info", "source", "ml_service2", "on", "13122", "867", "has", "shut",
                    "down"),
                    categorizationAnalyzer.tokenizeField("p2ps",
                            "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down."));

            assertEquals(Arrays.asList("vpxa", "49ec0b90", "verbose", "vpxahalcnxhostagent", "opid", "wfu", "ddeadb59",
                    "waitforupdatesdone", "received", "callback"),
                    categorizationAnalyzer.tokenizeField("vmware",
                            "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"));

            assertEquals(Arrays.asList("org.apache.coyote.http11", "http11baseprotocol", "destroy"),
                    categorizationAnalyzer.tokenizeField("apache",
                            "org.apache.coyote.http11.Http11BaseProtocol destroy"));

            assertEquals(Arrays.asList("1111529792", "info", "session", "45409105041220090733", "192.168.251.123", "proxy", "session",
                    "destroyed"),
                    categorizationAnalyzer.tokenizeField("proxy",
                            " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - " +
                                    "----------------- PROXY Session DESTROYED --------------------"));

            assertEquals(Arrays.asList("psyounggen", "total", "2572800k", "used", "1759355k", "0x0000000759500000", "0x0000000800000000",
                    "0x0000000800000000"),
                    categorizationAnalyzer.tokenizeField("java",
                            "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"));
        }
    }

    public void testCustomAnalyzer() throws IOException {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig config = new CategorizationAnalyzerConfig.Builder()
                .addCharFilter(ignoreStuffInSqaureBrackets)
                .setTokenizer("classic")
                .addTokenFilter("lowercase")
                .addTokenFilter(ignoreStuffThatBeginsWithADigit)
                .addTokenFilter("snowball")
                .build();
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, config)) {

            assertEquals(Arrays.asList("ml13-4608.1.p2ps", "info", "sourc", "ml_service2", "on", "has", "shut", "down"),
                    categorizationAnalyzer.tokenizeField("p2ps",
                            "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down."));

            assertEquals(Arrays.asList("vpxa", "receiv", "callback"),
                    categorizationAnalyzer.tokenizeField("vmware",
                            "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"));

            assertEquals(Arrays.asList("org.apache.coyote.http11.http11baseprotocol", "destroy"),
                    categorizationAnalyzer.tokenizeField("apache",
                            "org.apache.coyote.http11.Http11BaseProtocol destroy"));

            assertEquals(Arrays.asList("info", "session", "proxi", "session", "destroy"),
                    categorizationAnalyzer.tokenizeField("proxy",
                            " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - " +
                                    "----------------- PROXY Session DESTROYED --------------------"));

            assertEquals(Arrays.asList("psyounggen", "total", "use"),
                    categorizationAnalyzer.tokenizeField("java",
                            "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"));
        }
    }

    public void testEmptyString() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, defaultConfig)) {
            assertEquals(Collections.emptyList(), categorizationAnalyzer.tokenizeField("foo", ""));
        }
    }

    public void testThaiAnalyzer() throws IOException {
        CategorizationAnalyzerConfig config = new CategorizationAnalyzerConfig.Builder().setAnalyzer("thai").build();
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, config)) {

            // An example from the ES docs - no idea what it means or whether it's remotely sensible from a categorization point-of-view
            assertEquals(Arrays.asList("แสดง", "งาน", "ดี"),
                    categorizationAnalyzer.tokenizeField("thai",
                            "การที่ได้ต้องแสดงว่างานดี"));
        }
    }

    public void testInvalidAnalyzer() {
        CategorizationAnalyzerConfig config = new CategorizationAnalyzerConfig.Builder().setAnalyzer("does not exist").build();
        expectThrows(IllegalArgumentException.class, () -> new CategorizationAnalyzer(analysisRegistry, config));
    }
}
