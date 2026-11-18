/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.DatasetSource;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.datasource.grpc.EmployeeFlightServer;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;

/**
 * Integration tests for Arrow Flight (gRPC) external data source.
 * Starts an in-process Flight server serving employee data and runs csv-spec tests
 * against it through {@code FROM <dataset>}, registering an anonymous {@code flight} data source and one
 * dataset per {@code dataset:} directive.
 * <p>
 * Disabled: the planner refuses to read a dataset through a non-file source type, and the Flight plugin registers
 * no {@code DataSourceValidator}, so neither the data source nor its datasets can be created yet.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
@AwaitsFix(bugUrl = "https://github.com/elastic/esql-planning/issues/2059")
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

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(adminClient());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected List<String> indicesToLoad() {
        return List.of();
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
        List<URL> urls = classpathResources("/grpc-flight.csv-spec");
        return SpecReader.readScriptSpec(urls, CsvSpecReader::specParser);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        // run all tests
    }

    @Override
    protected void doTest() throws Throwable {
        String dataSource = DatasetRegistry.ensureDataSource(client(), "esql_spec_flight", "flight", Map.of("auth", "anonymous"));
        for (DatasetSource source : testCase.datasetSources) {
            DatasetRegistry.ensureDataset(client(), source.name(), dataSource, transformTemplates(source.resource()), source.withJson());
        }
        logger.debug("Flight dataset-mode query: {}", testCase.query);
        doTest(testCase.query);
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
