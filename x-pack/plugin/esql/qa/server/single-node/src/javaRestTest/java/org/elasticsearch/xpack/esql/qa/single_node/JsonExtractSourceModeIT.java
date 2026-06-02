/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@code JSON_EXTRACT} against every {@code _source} mode: default, stored, synthetic,
 * disabled, includes, excludes. Same two documents under each mapping, same probe battery.
 * Per-mode expectations inline.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class JsonExtractSourceModeIT extends RestEsqlTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @ParametersFactory(argumentFormatting = "%1s/%2s")
    public static List<Object[]> params() {
        return Arrays.stream(RestEsqlTestCase.Mode.values())
            .flatMap(m -> Arrays.stream(SourceMode.values()).map(s -> new Object[] { m, s }))
            .toList();
    }

    private final SourceMode sourceMode;

    public JsonExtractSourceModeIT(RestEsqlTestCase.Mode mode, SourceMode sourceMode) {
        super(mode);
        this.sourceMode = sourceMode;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testJsonExtract() throws IOException {
        String index = "json_extract_" + sourceMode.name().toLowerCase(java.util.Locale.ROOT);
        try {
            createIndexWithSourceMode(index, sourceMode);
        } catch (ResponseException e) {
            // Serverless ES rejects {"_source": {"enabled": false | "includes" | "excludes"}} mappings.
            // The DISABLED, INCLUDES_ADDRESS, and EXCLUDES_NAME cells exercise those exact features;
            // skip them when the cluster won't accept the mapping.
            assumeFalse(
                "cluster rejects _source." + sourceMode + " parameter: " + e.getMessage(),
                e.getResponse().getStatusLine().getStatusCode() == 400
                    && (e.getMessage().contains("[includes] is not allowed")
                        || e.getMessage().contains("[excludes] is not allowed")
                        || e.getMessage().contains("[enabled] is not allowed")
                        || e.getMessage().contains("[mode] is not allowed"))
            );
            throw e;
        }
        try {
            indexFixtureDocs(index);

            verifyTopLevelString(index);
            verifyTopLevelNumber(index);
            verifyNestedField(index);
            verifyArrayIndex(index);
            verifyObjectExtraction(index);
            verifyMissingPath(index);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + index));
            } catch (ResponseException ignored) {
                // best-effort cleanup
            }
        }
    }

    private void verifyTopLevelString(String index) throws IOException {
        // `name` is missing under DISABLED (null input), INCLUDES_ADDRESS (excluded), EXCLUDES_NAME (explicitly excluded).
        boolean present = sourceMode == SourceMode.DEFAULT || sourceMode == SourceMode.STORED || sourceMode == SourceMode.SYNTHETIC;
        var result = runProbe(index, "JSON_EXTRACT(_source, \"name\")", "n", warningsForMissing(present));
        if (present) {
            assertResult(result, "n", List.of(List.of("Alice"), List.of("Bob")));
        } else {
            assertResult(result, "n", List.of(nullList(), nullList()));
        }
    }

    private void verifyTopLevelNumber(String index) throws IOException {
        // `age` is missing under DISABLED (null input) and INCLUDES_ADDRESS (not in includes list).
        boolean present = sourceMode != SourceMode.DISABLED && sourceMode != SourceMode.INCLUDES_ADDRESS;
        var result = runProbe(index, "JSON_EXTRACT(_source, \"age\")", "a", warningsForMissing(present));
        if (present) {
            assertResult(result, "a", List.of(List.of("25"), List.of("30")));
        } else {
            assertResult(result, "a", List.of(nullList(), nullList()));
        }
    }

    private void verifyNestedField(String index) throws IOException {
        // `address.city` is present under every mode except DISABLED (null input).
        boolean present = sourceMode != SourceMode.DISABLED;
        var result = runProbe(index, "JSON_EXTRACT(_source, \"address.city\")", "city", warningsForMissing(present));
        if (present) {
            assertResult(result, "city", List.of(List.of("London"), List.of("Paris")));
        } else {
            assertResult(result, "city", List.of(nullList(), nullList()));
        }
    }

    private void verifyArrayIndex(String index) throws IOException {
        // `tags[0]` is present under every mode except DISABLED (null input).
        boolean present = sourceMode != SourceMode.DISABLED;
        var result = runProbe(index, "JSON_EXTRACT(_source, \"tags[0]\")", "t", warningsForMissing(present));
        if (present == false) {
            assertResult(result, "t", List.of(nullList(), nullList()));
            return;
        }
        if (sourceMode == SourceMode.SYNTHETIC) {
            // Synthetic reconstructs keyword arrays sorted + deduped, so Bob's ["user","guest"]
            // comes back as ["guest","user"]. Tracked at #149514.
            assertResult(result, "t", List.of(List.of("admin"), List.of("guest")));
        } else {
            assertResult(result, "t", List.of(List.of("admin"), List.of("user")));
        }
    }

    @SuppressWarnings("unchecked")
    private void verifyObjectExtraction(String index) throws IOException {
        // `address` is present under every mode except DISABLED.
        boolean present = sourceMode != SourceMode.DISABLED;
        var result = runProbe(index, "JSON_EXTRACT(_source, \"address\")", "addr", warningsForMissing(present));
        var values = (List<List<Object>>) result.get("values");
        assertThat(values.size(), equalTo(2));

        if (present == false) {
            assertThat(values.get(0).get(0), nullValue());
            assertThat(values.get(1).get(0), nullValue());
            return;
        }
        for (List<Object> row : values) {
            String addr = (String) row.get(0);
            assertThat("row=" + row, addr, notNullValue());
            // Synthetic source re-serializes — we don't pin field order or whitespace, just contents.
            assertThat(addr, containsString("\"city\""));
            assertThat(addr, containsString("\"zip\""));
        }
    }

    private void verifyMissingPath(String index) throws IOException {
        var result = runProbe(index, "JSON_EXTRACT(_source, \"definitely.not.here\")", "x", warningsForMissing(false));
        assertResult(result, "x", List.of(nullList(), nullList()));
    }

    /**
     * DISABLED → null-source IllegalStateException; path-missing → path-not-found
     * IllegalArgumentException; path present → no warning.
     */
    private AssertWarnings warningsForMissing(boolean pathPresent) {
        if (sourceMode == SourceMode.DISABLED) {
            return new AssertWarnings.AllowedRegexes(List.of(EVAL_WARNING, NULL_SOURCE_WARNING));
        }
        if (pathPresent) {
            return new AssertWarnings.NoWarnings();
        }
        return new AssertWarnings.AllowedRegexes(List.of(EVAL_WARNING, EVAL_WARNING_DETAIL));
    }

    private Map<String, Object> runProbe(String index, String expr, String column, AssertWarnings warnings) throws IOException {
        var query = """
            FROM %index METADATA _source
            | EVAL %column = %expr
            | KEEP %column
            | SORT %column NULLS LAST
            """.replace("%index", index).replace("%column", column).replace("%expr", expr);
        return run(query, warnings);
    }

    private Map<String, Object> run(String query, AssertWarnings warnings) throws IOException {
        return runEsql(requestObjectBuilder().query(query), warnings, profileLogger, mode);
    }

    @SuppressWarnings("unchecked")
    private static void assertResult(Map<String, Object> result, String column, List<List<Object>> expectedValues) {
        assertResultMap(result, List.of(Map.of("name", column, "type", "keyword")), expectedValues);
    }

    private static List<Object> nullList() {
        return java.util.Collections.singletonList(null);
    }

    private static final Pattern EVAL_WARNING = Pattern.compile(".*Line \\d+:\\d+: evaluation of \\[.*\\] failed.*");
    private static final Pattern EVAL_WARNING_DETAIL = Pattern.compile(".*java\\.lang\\.IllegalArgumentException.*");
    private static final Pattern NULL_SOURCE_WARNING = Pattern.compile(".*IllegalStateException.*_source is null.*");

    private static void indexFixtureDocs(String index) throws IOException {
        bulkIndex(index, """
            {"index":{"_id":"1"}}
            {"name":"Alice","age":30,"address":{"city":"London","zip":"EC1A"},"tags":["admin","user"]}
            {"index":{"_id":"2"}}
            {"name":"Bob","age":25,"address":{"city":"Paris","zip":"75001"},"tags":["user","guest"]}
            """);
    }

    private static void bulkIndex(String index, String body) throws IOException {
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertOK(response);
    }

    private static void createIndexWithSourceMode(String index, SourceMode sourceMode) throws IOException {
        Settings.Builder settings = Settings.builder();
        String mapping = sourceMode.mapping();
        sourceMode.applySettings(settings);
        createIndex(index, settings.build(), mapping);
    }

    /** Every supported {@code _source} configuration. Mappings differ only in the source clause. */
    enum SourceMode {
        DEFAULT {
            @Override
            String mapping() {
                return STANDARD_MAPPING;
            }
        },
        STORED {
            @Override
            void applySettings(Settings.Builder builder) {
                builder.put("index.mapping.source.mode", "stored");
            }

            @Override
            String mapping() {
                return STANDARD_MAPPING;
            }
        },
        SYNTHETIC {
            @Override
            void applySettings(Settings.Builder builder) {
                builder.put("index.mapping.source.mode", "synthetic");
            }

            @Override
            String mapping() {
                return STANDARD_MAPPING;
            }
        },
        DISABLED {
            @Override
            String mapping() {
                return """
                    "_source": { "enabled": false },
                    "properties": {
                      "name":    { "type": "keyword" },
                      "age":     { "type": "long" },
                      "address": { "properties": { "city": { "type": "keyword" }, "zip": { "type": "keyword" } } },
                      "tags":    { "type": "keyword" }
                    }
                    """;
            }
        },
        INCLUDES_ADDRESS {
            @Override
            String mapping() {
                return """
                    "_source": { "includes": ["address", "tags"] },
                    "properties": {
                      "name":    { "type": "keyword" },
                      "age":     { "type": "long" },
                      "address": { "properties": { "city": { "type": "keyword" }, "zip": { "type": "keyword" } } },
                      "tags":    { "type": "keyword" }
                    }
                    """;
            }
        },
        EXCLUDES_NAME {
            @Override
            String mapping() {
                return """
                    "_source": { "excludes": ["name"] },
                    "properties": {
                      "name":    { "type": "keyword" },
                      "age":     { "type": "long" },
                      "address": { "properties": { "city": { "type": "keyword" }, "zip": { "type": "keyword" } } },
                      "tags":    { "type": "keyword" }
                    }
                    """;
            }
        };

        void applySettings(Settings.Builder builder) {}

        abstract String mapping();

        private static final String STANDARD_MAPPING = """
            "properties": {
              "name":    { "type": "keyword" },
              "age":     { "type": "long" },
              "address": { "properties": { "city": { "type": "keyword" }, "zip": { "type": "keyword" } } },
              "tags":    { "type": "keyword" }
            }
            """;
    }
}
