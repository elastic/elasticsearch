/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Version;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;

/**
 * Integration tests for ClickBench-derived Parquet datasets. The {@link ClickBenchFixture} downloads
 * the first row group from 30 ClickHouse partitioned files at test startup, producing:
 * <ul>
 *   <li>A single-file dataset ({@code clickbench/hits.parquet}) — referenced as {@code {{clickbench_1file}}}</li>
 *   <li>A 5-file split dataset ({@code clickbench_multi/hits_*.parquet}) — referenced as {@code {{clickbench_5files}}}</li>
 * </ul>
 * Tests run against local {@code file://} URIs only. They skip gracefully when the remote ClickHouse
 * data is unreachable (e.g. air-gapped CI).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class ClickBenchParquetSpecIT extends EsqlSpecTestCase {

    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{(\\w+)}}");

    private static final ClickBenchFixture clickBenchFixture = new ClickBenchFixture();

    private static final ElasticsearchCluster cluster = Clusters.testCluster(() -> "http://localhost:0");

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(clickBenchFixture).around(cluster);

    public ClickBenchParquetSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @ParametersFactory(argumentFormatting = "clickbench:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/external-clickbench-*.csv-spec");
        assertTrue("No clickbench csv-spec files found", urls.size() > 0);
        return org.elasticsearch.xpack.esql.SpecReader.readScriptSpec(urls, specParser());
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        assumeTrue("ClickBench data not reachable", ClickBenchFixture.isDataReachable());
        assumeTrue("ClickBench fixture not ready", clickBenchFixture.fixturesRoot() != null);
        checkCapabilities(adminClient(), testFeatureService, testName, testCase);
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
    }

    @Override
    protected void doTest() throws Throwable {
        String query = testCase.query;
        query = substituteClickBenchTemplates(query);
        doTest(query);
    }

    @Override
    protected List<String> indicesToLoad() {
        return List.of();
    }

    private String substituteClickBenchTemplates(String query) {
        Path fixtures = clickBenchFixture.fixturesRoot();
        Matcher matcher = TEMPLATE_PATTERN.matcher(query);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String templateName = matcher.group(1);
            String replacement = resolveTemplate(templateName, fixtures);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private static String resolveTemplate(String templateName, Path fixturesRoot) {
        return switch (templateName) {
            case "clickbench_1file" -> fixturesRoot.resolve("clickbench").resolve("hits.parquet").toUri().toString();
            case "clickbench_5files" -> {
                String dirUri = fixturesRoot.resolve("clickbench_multi").toUri().toString();
                yield dirUri + "*.parquet";
            }
            default -> throw new IllegalArgumentException("Unknown ClickBench template: {{" + templateName + "}}");
        };
    }
}
