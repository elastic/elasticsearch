/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureResponse;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

@ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(
    value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE",
    reason = "debug lookup join expression resolution"
)
public class LookupJoinIT extends AbstractEsqlIntegTestCase {

    private static final String EMPLOYEES_INDEX = "employees";
    private static final String AGES_INDEX = "ages";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(EsqlPlugin.class);
        plugins.add(MapperExtrasPlugin.class);
        plugins.add(LocalStateEnrich.class);
        plugins.add(IngestCommonPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    public static class LocalStateEnrich extends LocalStateCompositeXPackPlugin {

        public LocalStateEnrich(final Settings settings, final Path configPath) throws Exception {
            super(settings, configPath);

            plugins.add(new EnrichPlugin(settings) {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return this.getLicenseState();
                }
            });
        }

        public static class EnrichTransportXPackInfoAction extends TransportXPackInfoAction {
            @Inject
            public EnrichTransportXPackInfoAction(
                TransportService transportService,
                ActionFilters actionFilters,
                LicenseService licenseService,
                NodeClient client
            ) {
                super(transportService, actionFilters, licenseService, client);
            }

            @Override
            protected List<ActionType<XPackInfoFeatureResponse>> infoActions() {
                return Collections.singletonList(XPackInfoFeatureAction.ENRICH);
            }
        }

        @Override
        protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
            return EnrichTransportXPackInfoAction.class;
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;  // Need real HTTP transport for RestClient
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Disable security to avoid security context issues with enrich policy actions
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    @Before
    public void setupEnrichPolicy() {
        // Load ages data and enrich policy before any test runs
        if (indexExists(AGES_INDEX) == false) {
            loadAgesData();
        }
        ensureAgesEnrichPolicy();
    }

    private void ensureIndicesAndData() {
        if (indexExists(EMPLOYEES_INDEX) == false) {
            loadEmployeesData();
        } else {
            // Index exists, but verify it has data
            refresh(EMPLOYEES_INDEX);
            ensureGreen(EMPLOYEES_INDEX);
        }
    }

    private void ensureAgesEnrichPolicy() {
        // Check if policy already exists
        boolean policyExists = false;
        try {
            var response = client().execute(
                GetEnrichPolicyAction.INSTANCE,
                new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "ages_policy")
            ).actionGet();
            var policies = response.getPolicies();
            policyExists = policies.stream().anyMatch(p -> p.getName().equals("ages_policy"));
        } catch (Exception e) {
            // Policy doesn't exist, will create it
        }

        if (policyExists == false) {
            // Create the policy fresh
            loadAgesEnrichPolicy();

            // Verify the policy was created successfully and is accessible
            try {
                var response = client().execute(
                    GetEnrichPolicyAction.INSTANCE,
                    new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "ages_policy")
                ).actionGet();
                var policies = response.getPolicies();
                boolean verified = policies.stream().anyMatch(p -> p.getName().equals("ages_policy"));
                if (verified == false) {
                    throw new AssertionError("Enrich policy was loaded but verification failed");
                }
            } catch (Exception e) {
                throw new AssertionError("Failed to verify enrich policy was created: " + e.getMessage(), e);
            }
        }
    }

    private void loadEmployeesData() {
        try {
            RestClient restClient = getRestClient();
            org.elasticsearch.logging.Logger logger = org.elasticsearch.logging.LogManager.getLogger(CsvTestsDataLoader.class);

            loadSingleDataset(
                restClient,
                logger,
                EMPLOYEES_INDEX,
                "mapping-default.json",
                "employees.csv",
                Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build()
            );
            // Refresh the index to make data available
            refresh(EMPLOYEES_INDEX);
            ensureGreen(EMPLOYEES_INDEX);
        } catch (IOException e) {
            throw new AssertionError("Failed to load employees CSV data", e);
        }
    }

    private void loadAgesData() {
        try {
            RestClient restClient = getRestClient();
            org.elasticsearch.logging.Logger logger = org.elasticsearch.logging.LogManager.getLogger(CsvTestsDataLoader.class);

            loadSingleDataset(
                restClient,
                logger,
                AGES_INDEX,
                "mapping-ages.json",
                "ages.csv",
                Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build()
            );
            // Refresh the index to make data available for enrich policy execution
            refresh(AGES_INDEX);
        } catch (IOException e) {
            throw new AssertionError("Failed to load ages CSV data", e);
        }
    }

    private void loadAgesEnrichPolicy() {
        // Ensure ages index exists and is ready
        if (indexExists(AGES_INDEX) == false) {
            throw new AssertionError("Ages index does not exist");
        }

        // Ensure ages index is refreshed before creating enrich policy
        refresh(AGES_INDEX);
        ensureGreen(AGES_INDEX);

        EnrichPolicy policy = new EnrichPolicy(
            "range",
            null, // query
            List.of(AGES_INDEX), // indices
            "age_range", // match_field
            List.of("description") // enrich_fields
        );

        client().execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "ages_policy", policy))
            .actionGet();

        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "ages_policy"))
            .actionGet();

        ensureGreen(); // Wait for enrich index to be ready
    }

    /**
     * Loads a single dataset following the CsvTestsDataLoader.load() pattern.
     * Creates the index and loads CSV data using the public loadCsvData() method.
     */
    private void loadSingleDataset(
        RestClient restClient,
        org.elasticsearch.logging.Logger logger,
        String indexName,
        String mappingFileName,
        String csvFileName,
        Settings indexSettings
    ) throws IOException {
        URL mappingResource = CsvTestsDataLoader.class.getResource("/" + mappingFileName);
        URL csvResource = CsvTestsDataLoader.class.getResource("/data/" + csvFileName);
        if (mappingResource == null || csvResource == null) {
            throw new IllegalArgumentException("Cannot find resources for " + indexName);
        }

        String mappingContent = CsvTestsDataLoader.readTextFile(mappingResource);
        ESRestTestCase.createIndex(restClient, indexName, indexSettings, mappingContent, null);

        // Use the public loadCsvData() method to reuse existing CSV parsing logic
        CsvTestsDataLoader.loadCsvData(restClient, indexName, csvResource, false, logger);
    }

    private EsqlQueryResponse runQuery(String query) {
        return run(syncEsqlQueryRequest(query));
    }

    /**
     * Verifies that the query results match the expected data.
     * @param actualValues The actual results from the query
     * @param expectedRows Expected rows, where each row is an array of expected values in column order
     */
    private void verifyResults(List<List<Object>> actualValues, Object[]... expectedRows) {
        if (actualValues.size() != expectedRows.length) {
            fail(
                String.format(
                    Locale.ROOT,
                    "Result count mismatch.%nExpected: %d rows%nActual: %d rows%n%nExpected results:%n%s%n%nActual results:%n%s",
                    expectedRows.length,
                    actualValues.size(),
                    formatRows(expectedRows),
                    formatRows(actualValues)
                )
            );
        }
        for (int i = 0; i < expectedRows.length; i++) {
            List<Object> actualRow = actualValues.get(i);
            Object[] expectedRow = expectedRows[i];
            if (actualRow.size() != expectedRow.length) {
                fail(
                    String.format(
                        Locale.ROOT,
                        "Row %d column count mismatch.%nExpected: %d columns%nActual: %d "
                            + "columns%n%nExpected row %d: %s%nActual row %d: %s%n%nAll "
                            + "expected results:%n%s%n%nAll actual results:%n%s",
                        i,
                        expectedRow.length,
                        actualRow.size(),
                        i,
                        formatRow(expectedRow),
                        i,
                        formatRow(actualRow),
                        formatRows(expectedRows),
                        formatRows(actualValues)
                    )
                );
            }
            for (int j = 0; j < expectedRow.length; j++) {
                if (Objects.equals(actualRow.get(j), expectedRow[j]) == false) {
                    fail(
                        String.format(
                            Locale.ROOT,
                            "Row %d, column %d mismatch.%nExpected: %s%nActual: %s%n%n"
                                + "Expected row %d: %s%nActual row %d: %s%n%nAll expected results:"
                                + "%n%s%n%nAll actual results:%n%s",
                            i,
                            j,
                            expectedRow[j],
                            actualRow.get(j),
                            i,
                            formatRow(expectedRow),
                            i,
                            formatRow(actualRow),
                            formatRows(expectedRows),
                            formatRows(actualValues)
                        )
                    );
                }
            }
        }
    }

    private String formatRows(Object[]... rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.length; i++) {
            sb.append("Row ").append(i).append(": ").append(formatRow(rows[i])).append("\n");
        }
        return sb.toString();
    }

    private String formatRows(List<List<Object>> rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            sb.append("Row ").append(i).append(": ").append(formatRow(rows.get(i))).append("\n");
        }
        return sb.toString();
    }

    private String formatRow(Object[] row) {
        return java.util.Arrays.toString(row);
    }

    private String formatRow(List<Object> row) {
        return row.toString();
    }

    // Test case ported from enrichAgesStatsYear in enrich.csv-spec

    public void testEnrichAgesStatsYear() {
        ensureIndicesAndData();

        // First, verify there's data in the employees index
        String simpleQuery = String.format(Locale.ROOT, "FROM %s | LIMIT 10", EMPLOYEES_INDEX);
        try (EsqlQueryResponse simpleResponse = runQuery(simpleQuery)) {
            List<List<Object>> simpleValues = getValuesList(simpleResponse);
            if (simpleValues.isEmpty()) {
                throw new AssertionError("No data found in employees index - simple SELECT returned 0 rows");
            }
        }

        // Verify enrich policy exists before running query
        try {
            var response = client().execute(
                GetEnrichPolicyAction.INSTANCE,
                new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "ages_policy")
            ).actionGet();
            var policies = response.getPolicies();
            boolean policyExists = policies.stream().anyMatch(p -> p.getName().equals("ages_policy"));
            if (policyExists == false) {
                throw new AssertionError("Enrich policy ages_policy does not exist before query execution");
            }
        } catch (Exception e) {
            throw new AssertionError("Failed to verify enrich policy exists before query: " + e.getMessage(), e);
        }

        String query = String.format(Locale.ROOT, """
            FROM %s
            | WHERE birth_date > "1960-01-01"
            | EVAL birth_year = DATE_EXTRACT("year", birth_date)
            | EVAL age = 2022 - birth_year
            | ENRICH ages_policy ON age WITH age_group = description
            | STATS count=count(age_group) BY age_group, birth_year
            | KEEP birth_year, age_group, count
            | SORT birth_year DESC
            """, EMPLOYEES_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            List<List<Object>> values = getValuesList(response);
            verifyResults(
                values,
                new Object[] { 1965L, "Middle-aged", 1L },
                new Object[] { 1964L, "Middle-aged", 4L },
                new Object[] { 1963L, "Middle-aged", 7L },
                new Object[] { 1962L, "Senior", 6L },
                new Object[] { 1961L, "Senior", 8L },
                new Object[] { 1960L, "Senior", 8L }
            );
        }
    }
}
