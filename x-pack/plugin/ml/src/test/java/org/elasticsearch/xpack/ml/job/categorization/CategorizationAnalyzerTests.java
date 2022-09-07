/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CategorizationAnalyzerTests extends ESTestCase {

    @SuppressWarnings("checkstyle:linelength")
    private static final String NGINX_ERROR_EXAMPLE =
        """
            a client request body is buffered to a temporary file /tmp/client-body/0000021894, client: 10.8.0.12, server: apm.35.205.226.121.ip.es.io, request: "POST /intake/v2/events HTTP/1.1", host: "apm.35.205.226.121.ip.es.io"
            10.8.0.12 - - [29/Nov/2020:21:34:55 +0000] "POST /intake/v2/events HTTP/1.1" 202 0 "-" "elasticapm-dotnet/1.5.1 System.Net.Http/4.6.28208.02 .NET_Core/2.2.8" 27821 0.002 [default-apm-apm-server-8200] [] 10.8.1.19:8200 0 0.001 202 f961c776ff732f5c8337530aa22c7216
            10.8.0.14 - - [29/Nov/2020:21:34:56 +0000] "POST /intake/v2/events HTTP/1.1" 202 0 "-" "elasticapm-python/5.10.0" 3594 0.002 [default-apm-apm-server-8200] [] 10.8.1.18:8200 0 0.001 202 61feb8fb9232b1ebe54b588b95771ce4
            10.8.4.90 - - [29/Nov/2020:21:34:56 +0000] "OPTIONS /intake/v2/rum/events HTTP/2.0" 200 0 "http://opbeans-frontend:3000/dashboard" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Cypress/3.3.1 Chrome/61.0.3163.100 Electron/2.0.18 Safari/537.36" 292 0.001 [default-apm-apm-server-8200] [] 10.8.1.19:8200 0 0.000 200 5fbe8cd4d217b932def1c17ed381c66b
            10.8.4.90 - - [29/Nov/2020:21:34:56 +0000] "POST /intake/v2/rum/events HTTP/2.0" 202 0 "http://opbeans-frontend:3000/dashboard" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Cypress/3.3.1 Chrome/61.0.3163.100 Electron/2.0.18 Safari/537.36" 3004 0.001 [default-apm-apm-server-8200] [] 10.8.1.18:8200 0 0.001 202 4735f571928595744ac6a9545c3ecdf5
            10.8.0.11 - - [29/Nov/2020:21:34:56 +0000] "POST /intake/v2/events HTTP/1.1" 202 0 "-" "elasticapm-node/3.8.0 elastic-apm-http-client/9.4.2 node/12.20.0" 4913 10.006 [default-apm-apm-server-8200] [] 10.8.1.18:8200 0 0.002 202 1eac41789ea9a60a8be4e476c54cbbc9
            10.8.0.14 - - [29/Nov/2020:21:34:57 +0000] "POST /intake/v2/events HTTP/1.1" 202 0 "-" "elasticapm-python/5.10.0" 1025 0.001 [default-apm-apm-server-8200] [] 10.8.1.18:8200 0 0.001 202 d27088936cadd3b8804b68998a5f94fa
            """;

    private AnalysisRegistry analysisRegistry;

    public static AnalysisRegistry buildTestAnalysisRegistry(Environment environment) throws Exception {
        CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin();
        MachineLearning ml = new MachineLearning(environment.settings());
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
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
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
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("Failed to find global analyzer [does not exist]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenValidCustomConfig() throws IOException {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().addCharFilter(ignoreStuffInSqaureBrackets)
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter(ignoreStuffThatBeginsWithADigit)
            .addTokenFilter("snowball");
        CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry);
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithInvalidCharFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().addCharFilter("wrong!")
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("failed to find global char_filter under [wrong!]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithMisconfiguredCharFilter() {
        Map<String, Object> noPattern = new HashMap<>();
        noPattern.put("type", "pattern_replace");
        noPattern.put("attern", "should have been pattern");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().addCharFilter(noPattern)
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("pattern is missing for [__anonymous__pattern_replace] char filter of type 'pattern_replace'", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithInvalidTokenizer() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().addCharFilter(ignoreStuffInSqaureBrackets)
            .setTokenizer("oops!")
            .addTokenFilter("lowercase")
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("failed to find global tokenizer under [oops!]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenNoTokenizer() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().addCharFilter(ignoreStuffInSqaureBrackets)
            .addTokenFilter("lowercase")
            .addTokenFilter(ignoreStuffThatBeginsWithADigit)
            .addTokenFilter("snowball");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("categorization_analyzer that is not a global analyzer must specify a [tokenizer] field", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithInvalidTokenFilter() {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().addCharFilter(ignoreStuffInSqaureBrackets)
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter("oh dear!");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("failed to find global filter under [oh dear!]", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenCustomConfigWithMisconfiguredTokenFilter() {
        Map<String, Object> noPattern = new HashMap<>();
        noPattern.put("type", "pattern_replace");
        noPattern.put("attern", "should have been pattern");
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().addCharFilter("html_strip")
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter(noPattern);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("pattern is missing for [__anonymous__pattern_replace] token filter of type 'pattern_replace'", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenAnalyzerAndCharFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().setAnalyzer("standard")
            .addCharFilter("html_strip");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [char_filter] field", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenAnalyzerAndTokenizer() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().setAnalyzer("standard")
            .setTokenizer("classic");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [tokenizer] field", e.getMessage());
    }

    public void testVerifyConfigBuilder_GivenAnalyzerAndTokenFilter() {
        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder().setAnalyzer("standard")
            .addTokenFilter("lowercase");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CategorizationAnalyzer.verifyConfigBuilder(builder, analysisRegistry)
        );
        assertEquals("categorization_analyzer that is a global analyzer cannot also specify a [filter] field", e.getMessage());
    }

    // The default categorization analyzer matches what the analyzer in the ML C++ does
    public void testDefaultCategorizationAnalyzer() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, defaultConfig)) {

            assertEquals(
                Arrays.asList("ml13-4608.1.p2ps", "Info", "Source", "ML_SERVICE2", "on", "has", "shut", "down"),
                categorizationAnalyzer.tokenizeField("p2ps", "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down.")
            );

            assertEquals(
                Arrays.asList(
                    "Vpxa",
                    "verbose",
                    "VpxaHalCnxHostagent",
                    "opID",
                    "WFU-ddeadb59",
                    "WaitForUpdatesDone",
                    "Received",
                    "callback"
                ),
                categorizationAnalyzer.tokenizeField(
                    "vmware",
                    "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"
                )
            );

            assertEquals(
                Arrays.asList("org.apache.coyote.http11.Http11BaseProtocol", "destroy"),
                categorizationAnalyzer.tokenizeField("apache", "org.apache.coyote.http11.Http11BaseProtocol destroy")
            );

            assertEquals(
                Arrays.asList("INFO", "session", "PROXY", "Session", "DESTROYED"),
                categorizationAnalyzer.tokenizeField(
                    "proxy",
                    " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - "
                        + "----------------- PROXY Session DESTROYED --------------------"
                )
            );

            assertEquals(
                Arrays.asList("PSYoungGen", "total", "used"),
                categorizationAnalyzer.tokenizeField(
                    "java",
                    "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"
                )
            );

            assertEquals(
                Arrays.asList(
                    "client",
                    "request",
                    "body",
                    "is",
                    "buffered",
                    "to",
                    "temporary",
                    "file",
                    "tmp",
                    "client-body",
                    "client",
                    "server",
                    "apm.35.205.226.121.ip.es.io",
                    "request",
                    "POST",
                    "intake",
                    "v2",
                    "events",
                    "HTTP",
                    "host",
                    "apm.35.205.226.121.ip.es.io",
                    "POST",
                    "intake",
                    "v2",
                    "events",
                    "HTTP",
                    "elasticapm-dotnet",
                    "System.Net.Http",
                    "NET_Core",
                    "default-apm-apm-server-8200",
                    "POST",
                    "intake",
                    "v2",
                    "events",
                    "HTTP",
                    "elasticapm-python",
                    "default-apm-apm-server-8200",
                    "OPTIONS",
                    "intake",
                    "v2",
                    "rum",
                    "events",
                    "HTTP",
                    "http",
                    "opbeans-frontend",
                    "dashboard",
                    "Mozilla",
                    "X11",
                    "Linux",
                    "x86_64",
                    "AppleWebKit",
                    "KHTML",
                    "like",
                    "Gecko",
                    "Cypress",
                    "Chrome",
                    "Electron",
                    "Safari",
                    "default-apm-apm-server-8200",
                    "POST",
                    "intake",
                    "v2",
                    "rum",
                    "events",
                    "HTTP",
                    "http",
                    "opbeans-frontend",
                    "dashboard",
                    "Mozilla",
                    "X11",
                    "Linux",
                    "x86_64",
                    "AppleWebKit",
                    "KHTML",
                    "like",
                    "Gecko",
                    "Cypress",
                    "Chrome",
                    "Electron",
                    "Safari",
                    "default-apm-apm-server-8200",
                    "POST",
                    "intake",
                    "v2",
                    "events",
                    "HTTP",
                    "elasticapm-node",
                    "elastic-apm-http-client",
                    "node",
                    "default-apm-apm-server-8200",
                    "POST",
                    "intake",
                    "v2",
                    "events",
                    "HTTP",
                    "elasticapm-python",
                    "default-apm-apm-server-8200"
                ),
                categorizationAnalyzer.tokenizeField("nginx_error", NGINX_ERROR_EXAMPLE)
            );
        }
    }

    public void testDefaultCategorizationAnalyzerWithCategorizationFilter() throws IOException {
        // A categorization filter that removes stuff in square brackets
        CategorizationAnalyzerConfig defaultConfigWithCategorizationFilter = CategorizationAnalyzerConfig
            .buildDefaultCategorizationAnalyzer(Collections.singletonList("\\[[^\\]]*\\]"));
        try (
            CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(
                analysisRegistry,
                defaultConfigWithCategorizationFilter
            )
        ) {

            assertEquals(
                Arrays.asList("ml13-4608.1.p2ps", "Info", "Source", "ML_SERVICE2", "on", "has", "shut", "down"),
                categorizationAnalyzer.tokenizeField("p2ps", "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down.")
            );

            assertEquals(
                Arrays.asList("Vpxa", "Received", "callback"),
                categorizationAnalyzer.tokenizeField(
                    "vmware",
                    "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"
                )
            );

            assertEquals(
                Arrays.asList("org.apache.coyote.http11.Http11BaseProtocol", "destroy"),
                categorizationAnalyzer.tokenizeField("apache", "org.apache.coyote.http11.Http11BaseProtocol destroy")
            );

            assertEquals(
                Arrays.asList("INFO", "session", "PROXY", "Session", "DESTROYED"),
                categorizationAnalyzer.tokenizeField(
                    "proxy",
                    " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - "
                        + "----------------- PROXY Session DESTROYED --------------------"
                )
            );

            assertEquals(
                Arrays.asList("PSYoungGen", "total", "used"),
                categorizationAnalyzer.tokenizeField(
                    "java",
                    "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"
                )
            );
        }
    }

    public void testMlStandardCategorizationAnalyzer() throws IOException {
        CategorizationAnalyzerConfig standardConfig = CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(null);
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, standardConfig)) {

            assertEquals(
                Arrays.asList("ml13-4608.1.p2ps", "Info", "Source", "ML_SERVICE2", "on", "has", "shut", "down"),
                categorizationAnalyzer.tokenizeField("p2ps", "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down.")
            );

            assertEquals(
                Arrays.asList(
                    "Vpxa",
                    "verbose",
                    "VpxaHalCnxHostagent",
                    "opID",
                    "WFU-ddeadb59",
                    "WaitForUpdatesDone",
                    "Received",
                    "callback"
                ),
                categorizationAnalyzer.tokenizeField(
                    "vmware",
                    "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"
                )
            );

            assertEquals(
                Arrays.asList("org.apache.coyote.http11.Http11BaseProtocol", "destroy"),
                categorizationAnalyzer.tokenizeField("apache", "org.apache.coyote.http11.Http11BaseProtocol destroy")
            );

            assertEquals(
                Arrays.asList("INFO", "session", "PROXY", "Session", "DESTROYED"),
                categorizationAnalyzer.tokenizeField(
                    "proxy",
                    " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - "
                        + "----------------- PROXY Session DESTROYED --------------------"
                )
            );

            assertEquals(
                Arrays.asList("PSYoungGen", "total", "used"),
                categorizationAnalyzer.tokenizeField(
                    "java",
                    "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"
                )
            );

            assertEquals(
                Arrays.asList("first", "line"),
                categorizationAnalyzer.tokenizeField("multiline", "first line\nsecond line\nthird line")
            );

            assertEquals(
                Arrays.asList("first", "line"),
                categorizationAnalyzer.tokenizeField("windows_multiline", "first line\r\nsecond line\r\nthird line")
            );

            assertEquals(
                Arrays.asList("second", "line"),
                categorizationAnalyzer.tokenizeField("multiline_first_blank", "\nsecond line\nthird line")
            );

            assertEquals(
                Arrays.asList("second", "line"),
                categorizationAnalyzer.tokenizeField("windows_multiline_first_blank", "\r\nsecond line\r\nthird line")
            );

            assertEquals(
                Arrays.asList(
                    "client",
                    "request",
                    "body",
                    "is",
                    "buffered",
                    "to",
                    "temporary",
                    "file",
                    "/tmp/client-body/0000021894",
                    "client",
                    "server",
                    "apm.35.205.226.121.ip.es.io",
                    "request",
                    "POST",
                    "/intake/v2/events",
                    "HTTP/1.1",
                    "host",
                    "apm.35.205.226.121.ip.es.io"
                ),
                categorizationAnalyzer.tokenizeField("nginx_error", NGINX_ERROR_EXAMPLE)
            );

            // Limited to CategorizationAnalyzerConfig.MAX_TOKEN_COUNT (100) tokens
            assertEquals(
                Arrays.asList(
                    "took",
                    "errors",
                    "true",
                    "items",
                    "create",
                    "index",
                    "internal.alerts-security.alerts-default-000003",
                    "id",
                    "status",
                    "error",
                    "type",
                    "mapper_parsing_exception",
                    "reason",
                    "failed",
                    "to",
                    "parse",
                    "field",
                    "process.parent.command_line",
                    "of",
                    "type",
                    "wildcard",
                    "in",
                    "document",
                    "with",
                    "id",
                    "Preview",
                    "of",
                    "field",
                    "s",
                    "value",
                    "R",
                    "Q",
                    "M",
                    "t",
                    "k",
                    "P",
                    "r",
                    "o",
                    "T",
                    "g",
                    "I",
                    "r",
                    "R",
                    "N",
                    "P",
                    "R",
                    "I",
                    "N",
                    "t",
                    "n",
                    "i",
                    "l",
                    "n",
                    "r",
                    "T",
                    "R",
                    "i",
                    "t",
                    "w",
                    "n",
                    "t",
                    "s",
                    "i",
                    "i",
                    "l",
                    "r",
                    "t",
                    "s",
                    "M",
                    "i",
                    "Q",
                    "N",
                    "L",
                    "t",
                    "v",
                    "r",
                    "s",
                    "s",
                    "w",
                    "o",
                    "n",
                    "i",
                    "n",
                    "m",
                    "i",
                    "r",
                    "g",
                    "n",
                    "i",
                    "i",
                    "l",
                    "n",
                    "o",
                    "n",
                    "i",
                    "r",
                    "t",
                    "u",
                    "k",
                    "l"
                ),
                categorizationAnalyzer.tokenizeField(
                    "multiline",
                    "{\"took\":529,\"errors\":true,\"items\":[{\"create\":{\"_index\":\".internal.alerts-security.alerts-default-000003\","
                        + "\"_id\":\"5b87a604eb4b7bf22a74d3bdda7855f64080a3c620d13563ed000f64670964a8\",\"status\":400,\"error\":{\"type\":"
                        + "\"mapper_parsing_exception\",\"reason\":\"failed to parse field [process.parent.command_line] of type [wildcard]"
                        + " in document with id '5b87a604eb4b7bf22a74d3bdda7855f64080a3c620d13563ed000f64670964a8'. Preview of field's"
                        + " value: '{1220=B, 1217=R, 1216=/, 1215=b, 1214=d, 1213=/, 1212=Q, 1211=M, 1210=t, 1219=B, 1218=A, 1231=k,"
                        + " 1230=-, 0=\\\", 1228= , 1=C, 1227=\\\", 2=:, 1226=\\\\, 3=\\\\, 1225=\\\", 4=P, 1224=1, 5=r, 1223=~, 6=o,"
                        + " 1222=T, 7=g, 1221=I, 8=r, 800=R, 9=a, 801=A, 802=~, 803=3, 804=/, 805=N, 806=P, 807=R, 1229= , 808=I, 809=N,"
                        + " 1000=\\\\, 1242=_, 1241=t, 1240=e, 1239=n, 1238=i, 1237= , 1236=l, 1235=e, 1234=n, 1233=r, 810=T, 1232=e,"
                        + " 811=~, 812=1, 813=/, 814=R, 815=a, 816=b, 817=b, 818=i, 819=t, 1011=w, 1253=n, 1010= , 1252=e, 1251=t, 1250=s,"
                        + " 1008=i, 1007=b, 1249=i, 1006=b, 1248=l, 1005=a, 1247=_, 1004=r, 1246=t, 1003=-, 1245=s, 820=M, 1002= ,"
                        + " 1244=i, 821=Q, 1001=\\\", 1243=d, 822=/, 823=E, 824=N, 825=A, 826=B, 827=L, 828=E, 829=~, 1009=t, 1022=v,"
                        + " 1264= , 1021=r, 1263=2, 1020=e, 1262=7, 1261=6, 1260=5, 1019=s, 1018=_, 1017=s, 1259=2, 1016=w, 1258= ,"
                        + " 1015=o, 1257=n, 830=1, 1014=d, 1256=i,  831=\\\", 1013=n, 1255=m, 832=\\\\, 1012=i, 1254=_, 833=\\\","
                        + " 834= , 835=-, 836=r, 837=a, 838=b, 839=b, 1033= , 1275=e, 1032=g, 1274=n, 1031=i, 1273=i, 1030=f,"
                        + " 1272= , 1271=l, 1270=e, 1029=n, 1028=o, 1027=c, 1269=n, 840=i, 1026=_, 1268=r, 841=t, 1025=e, 1267=e,"
                        + " 600=u, 842= , 1024=c, 1266=k, 601=l, 843=p, 1023=i, 1265=-, 602=t, 844=l, 603=_, 845=u, 604=f, 846=g, 605=i,"
                        + " 847=i, 606=l, 848=n, 607=e, 849=s, 608= , 609=\\\\, 1044=R, 1286=t, 1043=G, 1285=s, 1042=O, 1284=i, 1041=R,"
                        + " 1283=l, 1040=P, 1282=_, 1281=t, 1280=s, 1039=/, 850=_, 1038=:, 851=d, 1037=C, 1279=i, 610=\\\", 852=i,"
                        + " 1036=\\\", 1278=d, 611=\\\", 853=r, 1035=\\\", 1277=_, 612=C, 854= , 1034=\\\\, 1276=t, 613=:,"
                        + " 855=\\\\, 614=/, 856=\\\", 615=P, 857=\\\", 616=R, 858=C, 617=O, 859=:, 618=G, 619=R, 1055=~, 1297=7, 1054=T,"
                        + " 1296=6, 1053=N, 1295=5, 1052=I, 1294=2, 1051=R, 1293= , 1050=P, 1292=x, 1291=a, 1290=m, 860=/, 861=P, 1049=N,"
                        + " 620=A, 862=R, 1048=/, 621=~, 863=O, 1047=3, 1289=_, 622=3, 864=G, 1046=~, 1288=n, 623=/, 865=R, 1045=A, 1287=e,"
                        + " 624=N, 866=A, 625=P, 867=~, 626=R, 868=1, 627=I, 869=/, 628=N, 629=T, 1066=/, 1065=Q, 1064=M, 1063=t, 1062=i,"
                        + " 1061=b, 1060=b, 870=N, 871=P, 630=~, 872=R, 631=1, 873=I, 1059=a, 632=/, 874=N, 1058=R, 633=R, 875=T, 1057=/,"
                        + " 1299= , 634=a, 876=~, 1056=1, 1298=2, 635=b, 877=1, 636=b, 878=/, 637=i, 879=R, 638=t, 639=M, 1077=o, 1076=c,"
                        + " 1075=., 1074=q, 1073=m, 1072=t, 1071=i, 1070=b, 880=A, 881=B, 640=Q, 882=B, 641=/, 883=I, 400=1, 642=l, 884=T,"
                        + " 401=2, 643=o, 885=~, 1069=b, 402=8, 644=g, 886=1, 1068=a, 403=0, 645=/, 887=., 1067=r, 404=0, 646=R, 888=1,"
                        + " 405=0, 647=A, 889=0, 406= , 648=B, 407= , 649=B, 408=-, 409=k, 1080=i, 1088=s, 1087=o, 1086=-, 1085= ,"
                        + " 1084=\\\", 1083=\\\\, 1082=\\\", 890=/, 1081=g, 891=p, 650=I, 892=l, 651=T, 893=u, 410=e, 652=~, 894=g,"
                        + " 411=r, 653=1, 895=i, 412=n, 654=., 896=n, 413=e, 655=L, 897=s, 1079=f, 414=l, 656=O, 898=\\\", 1078=n,"
                        + " 415= , 657=G, 899=\\\\, 416=i, 658=\\\", 417=n, 659=\\\\, 418=e, 419=t, 1091=o, 1090=m, 1099=_, 1098=t,"
                        + " 1097=r, 1096=a, 1095=t, 1094=s, 1093= , 1092=n, 660=\\\", 661= , 420=_, 662=-, 421=d, 663=r, 422=e, 664=a,"
                        + " 423=f, 665=b, 424=a, 666=b, 425=u, 667=i, 1089=_, 426=l, 668=t, 427=t, 669= , 428=_, 429=c, 670=l, 671=a,"
                        + " 430=o, 672=g, 431=n, 673=e, 432=n, 674=r, 433=e, 675=_, 434=c, 676=u, 435=t, 677=p, 436=_, 678=g, 437=o,"
                        + " 679=r, 438=p, 439=t, 680=a, 681=d, 440=i, 682=e, 441=o, 683=_, 200= , 442=n, 684=f, 201=s, 443=s, 685=i,"
                        + " 202=t, 444= , 686=l, 203=a, 445=\\\", 687=e, 204=r, 446=[, 688= , 205=t, 447={, 689=\\\\, 206=_, 448=n,"
                        + " 207=s, 449=o, 208=a, 209=s, 690=\\\", 691=\\\", 450=d, 692=C, 451=e, 693=:, 210=l,"
                )
            );
        }
    }

    // The Elasticsearch standard analyzer - this is the default for indexing in Elasticsearch, but
    // NOT for ML categorization (and you'll see why if you look at the expected results of this test!)
    public void testStandardAnalyzer() throws IOException {
        CategorizationAnalyzerConfig config = new CategorizationAnalyzerConfig.Builder().setAnalyzer("standard").build();
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, config)) {

            assertEquals(
                Arrays.asList("ml13", "4608.1", "p2ps", "info", "source", "ml_service2", "on", "13122", "867", "has", "shut", "down"),
                categorizationAnalyzer.tokenizeField("p2ps", "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down.")
            );

            assertEquals(
                Arrays.asList(
                    "vpxa",
                    "49ec0b90",
                    "verbose",
                    "vpxahalcnxhostagent",
                    "opid",
                    "wfu",
                    "ddeadb59",
                    "waitforupdatesdone",
                    "received",
                    "callback"
                ),
                categorizationAnalyzer.tokenizeField(
                    "vmware",
                    "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"
                )
            );

            assertEquals(
                Arrays.asList("org.apache.coyote.http11", "http11baseprotocol", "destroy"),
                categorizationAnalyzer.tokenizeField("apache", "org.apache.coyote.http11.Http11BaseProtocol destroy")
            );

            assertEquals(
                Arrays.asList("1111529792", "info", "session", "45409105041220090733", "192.168.251.123", "proxy", "session", "destroyed"),
                categorizationAnalyzer.tokenizeField(
                    "proxy",
                    " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - "
                        + "----------------- PROXY Session DESTROYED --------------------"
                )
            );

            assertEquals(
                Arrays.asList(
                    "psyounggen",
                    "total",
                    "2572800k",
                    "used",
                    "1759355k",
                    "0x0000000759500000",
                    "0x0000000800000000",
                    "0x0000000800000000"
                ),
                categorizationAnalyzer.tokenizeField(
                    "java",
                    "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"
                )
            );
        }
    }

    public void testCustomAnalyzer() throws IOException {
        Map<String, Object> ignoreStuffInSqaureBrackets = new HashMap<>();
        ignoreStuffInSqaureBrackets.put("type", "pattern_replace");
        ignoreStuffInSqaureBrackets.put("pattern", "\\[[^\\]]*\\]");
        Map<String, Object> ignoreStuffThatBeginsWithADigit = new HashMap<>();
        ignoreStuffThatBeginsWithADigit.put("type", "pattern_replace");
        ignoreStuffThatBeginsWithADigit.put("pattern", "^[0-9].*");
        CategorizationAnalyzerConfig config = new CategorizationAnalyzerConfig.Builder().addCharFilter(ignoreStuffInSqaureBrackets)
            .setTokenizer("classic")
            .addTokenFilter("lowercase")
            .addTokenFilter(ignoreStuffThatBeginsWithADigit)
            .addTokenFilter("snowball")
            .build();
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, config)) {

            assertEquals(
                Arrays.asList("ml13-4608.1.p2ps", "info", "sourc", "ml_service2", "on", "has", "shut", "down"),
                categorizationAnalyzer.tokenizeField("p2ps", "<ml13-4608.1.p2ps: Info: > Source ML_SERVICE2 on 13122:867 has shut down.")
            );

            assertEquals(
                Arrays.asList("vpxa", "receiv", "callback"),
                categorizationAnalyzer.tokenizeField(
                    "vmware",
                    "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"
                )
            );

            assertEquals(
                Arrays.asList("org.apache.coyote.http11.http11baseprotocol", "destroy"),
                categorizationAnalyzer.tokenizeField("apache", "org.apache.coyote.http11.Http11BaseProtocol destroy")
            );

            assertEquals(
                Arrays.asList("info", "session", "proxi", "session", "destroy"),
                categorizationAnalyzer.tokenizeField(
                    "proxy",
                    " [1111529792] INFO  session <45409105041220090733@192.168.251.123> - "
                        + "----------------- PROXY Session DESTROYED --------------------"
                )
            );

            assertEquals(
                Arrays.asList("psyounggen", "total", "use"),
                categorizationAnalyzer.tokenizeField(
                    "java",
                    "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"
                )
            );
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
            assertEquals(Arrays.asList("แสดง", "งาน", "ดี"), categorizationAnalyzer.tokenizeField("thai", "การที่ได้ต้องแสดงว่างานดี"));
        }
    }

    public void testInvalidAnalyzer() {
        CategorizationAnalyzerConfig config = new CategorizationAnalyzerConfig.Builder().setAnalyzer("does not exist").build();
        expectThrows(IllegalArgumentException.class, () -> new CategorizationAnalyzer(analysisRegistry, config));
    }
}
