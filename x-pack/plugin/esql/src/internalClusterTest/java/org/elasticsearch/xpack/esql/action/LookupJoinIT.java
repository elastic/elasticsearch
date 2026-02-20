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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

@ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(
    value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE",
    reason = "debug lookup join expression resolution"
)

/**
 * This test is used to test LOOKUP JOIN and ENRICH functionality in ESQL.
 * It is similar to the csv-spec tests, but loads only the needed indices and policies
 * for the current specific test case and on demand.
 * This allows rapid debugging of certain test cases without waiting for all indices to load
 */
public class LookupJoinIT extends AbstractEsqlIntegTestCase {

    private static final Logger logger = LogManager.getLogger(LookupJoinIT.class);

    // Index name constants
    private static final String EMPLOYEES_INDEX = "employees";
    private static final String AGES_INDEX = "ages";
    private static final String BOOKS_INDEX = "books";
    private static final String DATE_NANOS_INDEX = "date_nanos";
    private static final String LANGUAGES_INDEX = "languages";
    private static final String LANGUAGES_LOOKUP_INDEX = "languages_lookup";
    private static final String LANGUAGES_MIXED_NUMERICS_INDEX = "languages_mixed_numerics";
    private static final String MESSAGE_TYPES_LOOKUP_INDEX = "message_types_lookup";

    // Enrich policy name constants
    private static final String AGES_POLICY = "ages_policy";
    private static final String LANGUAGES_POLICY = "languages_policy";

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

    /**
     * Ensures the specified indices exist, loading them if they don't.
     * Uses CsvTestsDataLoader.CSV_DATASET_MAP to get index definitions.
     * Only loads the indices that are explicitly requested.
     */
    private void ensureIndices(List<String> indexNames) throws IOException {
        RestClient restClient = getRestClient();

        for (String indexName : indexNames) {
            CsvTestsDataLoader.TestDataset dataset = CsvTestsDataLoader.CSV_DATASET_MAP.get(indexName);
            if (dataset == null) {
                throw new IllegalArgumentException("No definition found for index: " + indexName);
            }

            if (indexExists(indexName) == false) {
                // Ensure standard settings for test indices
                var indexSettings = Settings.builder()
                    .put(dataset.loadSettings())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();

                // Create index - let exceptions propagate so test fails immediately
                ESRestTestCase.createIndex(restClient, indexName, indexSettings, dataset.loadMappings(), null);

                // Load CSV data if available
                if (dataset.dataFileName() != null) {
                    CsvTestsDataLoader.loadCsvData(restClient, indexName, dataset.streamData(), dataset.allowSubFields());
                }

                refresh(indexName);
                ensureGreen(indexName);
            } else {
                // Index exists, but verify it's ready
                refresh(indexName);
                ensureGreen(indexName);
            }
        }
    }

    /**
     * Ensures the specified enrich policies exist, creating and executing them if they don't.
     * Uses CsvTestsDataLoader.ENRICH_POLICIES to get policy definitions.
     */
    private void ensureEnrichPolicies(List<String> policyNames) throws IOException {
        RestClient restClient = getRestClient();

        // Build a map of policy name to EnrichConfig for quick lookup
        Map<String, CsvTestsDataLoader.EnrichConfig> policyMap = CsvTestsDataLoader.ENRICH_POLICIES.stream()
            .collect(Collectors.toMap(CsvTestsDataLoader.EnrichConfig::policyName, Function.identity()));

        for (String policyName : policyNames) {
            CsvTestsDataLoader.EnrichConfig config = policyMap.get(policyName);
            if (config == null) {
                throw new IllegalArgumentException("No definition found for enrich policy: " + policyName);
            }

            // Check if policy already exists
            var response = client().execute(
                GetEnrichPolicyAction.INSTANCE,
                new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName)
            ).actionGet();
            var existingPolicies = response.getPolicies();
            boolean policyExists = existingPolicies.stream().anyMatch(p -> p.getName().equals(policyName));

            // Check if enrich index exists (the alias .enrich-{policy_name})
            boolean enrichIndexExists = indexExists(".enrich-" + policyName);

            if (policyExists == false) {
                // Use CsvTestsDataLoader to load the enrich policy (it handles both creation and execution)
                CsvTestsDataLoader.loadEnrichPolicy(restClient, config);
                ensureGreen(); // Wait for enrich index to be ready
            }
            if (enrichIndexExists == false) {
                client().execute(
                    ExecuteEnrichPolicyAction.INSTANCE,
                    new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policyName)
                ).actionGet();
                ensureGreen(); // Wait for enrich index to be ready
            }
        }
    }

    private EsqlQueryResponse runQuery(String query) {
        return run(syncEsqlQueryRequest(query));
    }

    // Test case ported from enrichAgesStatsYear in enrich.csv-spec
    public void testEnrichAgesStatsYear() throws IOException {
        // Required indices and enrich policies for this test
        ensureIndices(List.of(EMPLOYEES_INDEX, AGES_INDEX));
        ensureEnrichPolicies(List.of(AGES_POLICY));

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
            assertValues(
                response.values(),
                List.of(
                    List.of(1965L, "Middle-aged", 1L),
                    List.of(1964L, "Middle-aged", 4L),
                    List.of(1963L, "Middle-aged", 7L),
                    List.of(1962L, "Senior", 6L),
                    List.of(1961L, "Senior", 8L),
                    List.of(1960L, "Senior", 8L)
                )
            );
        }
    }

    // Test ported from csv-spec:lookup-join.FloatJoinScaledFloat
    // Tests LOOKUP JOIN with mixed numeric fields, specifically float to scaled_float conversion
    public void testFloatJoinScaledFloat() throws IOException {
        // Required indices and enrich policies for this test
        ensureIndices(List.of(LANGUAGES_MIXED_NUMERICS_INDEX));

        // Run the query
        String query = String.format(Locale.ROOT, """
            FROM %s
            | WHERE language_code_float IS NOT NULL
            | EVAL language_code_scaled_float = language_code_float
            | LOOKUP JOIN %s ON language_code_scaled_float
            | SORT language_code_scaled_float, language_name
            | KEEP language_code_scaled_float, language_name
            """, LANGUAGES_MIXED_NUMERICS_INDEX, LANGUAGES_MIXED_NUMERICS_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            assertValues(
                response.values(),
                List.of(
                    List.of(-3.4028234663852886E38, "min_long"),
                    List.of(-3.4028234663852886E38, "min_long_minus_1"),
                    List.of(-9.223372036854776E18, "min_long"),
                    List.of(-9.223372036854776E18, "min_long"),
                    List.of(-9.223372036854776E18, "min_long_minus_1"),
                    List.of(-9.223372036854776E18, "min_long_minus_1"),
                    List.of(-2.147483648E9, "min_int"),
                    List.of(-2.147483648E9, "min_int"),
                    List.of(-65505.0, "min_half_float_minus_1"),
                    List.of(-65504.0, "min_half_float"),
                    List.of(-32769.0, "min_short_minus_1"),
                    List.of(-32768.0, "min_short"),
                    List.of(-129.0, "min_byte_minus_1"),
                    List.of(-128.0, "min_byte"),
                    List.of(1.0, "English"),
                    List.of(2.0, "French"),
                    List.of(3.0, "Spanish"),
                    List.of(4.0, "German"),
                    List.of(127.0, "max_byte"),
                    List.of(128.0, "max_byte_plus_1"),
                    List.of(32767.0, "max_short"),
                    List.of(32768.0, "max_short_plus_1"),
                    List.of(65504.0, "max_half_float"),
                    List.of(65505.0, "max_half_float_plus_1"),
                    List.of(2.147483648E9, "max_int_plus_1"),
                    List.of(2.147483648E9, "max_int_plus_1"),
                    List.of(2.147483648E9, "max_int_plus_1"),
                    List.of(9.223372036854776E18, "max_long"),
                    List.of(9.223372036854776E18, "max_long"),
                    List.of(9.223372036854776E18, "max_long"),
                    List.of(9.223372036854776E18, "max_long_minus_1"),
                    List.of(9.223372036854776E18, "max_long_minus_1"),
                    List.of(9.223372036854776E18, "max_long_minus_1"),
                    List.of(9.223372036854776E18, "max_long_plus_1"),
                    List.of(9.223372036854776E18, "max_long_plus_1"),
                    List.of(9.223372036854776E18, "max_long_plus_1"),
                    List.of(3.4028234663852886E38, "max_long"),
                    List.of(3.4028234663852886E38, "max_long_minus_1"),
                    List.of(3.4028234663852886E38, "max_long_plus_1")
                )
            );
        }
    }

    // Test ported from csv-spec:lookup-join.lookupMessageFromRowWithShadowing
    // Tests LOOKUP JOIN with shadowing - the type field gets replaced by the lookup result
    public void testLookupMessageFromRowWithShadowing() throws IOException {
        // Required indices and enrich policies for this test
        ensureIndices(List.of(MESSAGE_TYPES_LOOKUP_INDEX));

        // Run the query
        String query = String.format(Locale.ROOT, """
            ROW left = "left", message = "Connected to 10.1.0.1", type = "unknown", right = "right"
            | LOOKUP JOIN %s ON message
            """, MESSAGE_TYPES_LOOKUP_INDEX);

        try (EsqlQueryResponse response = runQuery(query)) {
            assertValues(response.values(), List.of(List.of("left", "Connected to 10.1.0.1", "right", "Success")));
        }
    }
}
