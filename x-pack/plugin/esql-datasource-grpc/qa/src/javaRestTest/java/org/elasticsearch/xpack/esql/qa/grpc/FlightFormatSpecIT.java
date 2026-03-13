/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.datasource.grpc.EmployeeFlightServer;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;

/**
 * Integration tests for Arrow Flight (gRPC) external data source.
 * Starts an in-process Flight server serving employee data and runs csv-spec tests
 * against it via the EXTERNAL command.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FlightFormatSpecIT extends EsqlSpecTestCase {

    private static final Logger logger = LogManager.getLogger(FlightFormatSpecIT.class);
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{(\\w+)}}");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    private static EmployeeFlightServer flightServer;

    @BeforeClass
    public static void startFlightServer() throws IOException {
        flightServer = new EmployeeFlightServer(0);
        logger.info("Started Flight server on port {}", flightServer.port());
    }

    @AfterClass
    public static void stopFlightServer() throws IOException {
        if (flightServer != null) {
            flightServer.close();
            flightServer = null;
        }
    }

    @SuppressForbidden(reason = "need reflective access to skip standard data loading for external source tests")
    @BeforeClass
    public static void skipStandardDataLoading() {
        try {
            java.lang.reflect.Field ingestField = EsqlSpecTestCase.class.getDeclaredField("INGEST");
            ingestField.setAccessible(true);
            Object ingest = ingestField.get(null);

            java.lang.reflect.Field completedField = ingest.getClass().getDeclaredField("completed");
            completedField.setAccessible(true);
            completedField.setBoolean(ingest, true);

            logger.info("Skipped standard test data loading for Flight tests");
        } catch (Exception e) {
            logger.warn("Failed to skip standard data loading, tests may be slower", e);
        }
    }

    public FlightFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/external-grpc.csv-spec");
        return SpecReader.readScriptSpec(urls, CsvSpecReader.specParser());
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        // run all tests
    }

    @Override
    protected void doTest() throws Throwable {
        String query = testCase.query;
        query = transformTemplates(query);
        logger.debug("Transformed query for Flight backend: {}", query);
        doTest(query);
    }

    private String transformTemplates(String query) {
        Matcher matcher = TEMPLATE_PATTERN.matcher(query);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String templateName = matcher.group(1);
            String flightUri = "flight://localhost:" + flightServer.port() + "/" + templateName;
            matcher.appendReplacement(result, Matcher.quoteReplacement(flightUri));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }
}
