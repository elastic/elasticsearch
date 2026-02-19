/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.CsvTestUtils.COMMA_ESCAPING_REGEX;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ESCAPED_COMMA_SEQUENCE;
import static org.elasticsearch.xpack.esql.CsvTestUtils.multiValuesAwareCsvToStringArray;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.reader;

public class CsvTestsDataLoader {

    static {
        // Ensure the logging factory is initialized before the static logger field below. When running standalone (via main() or
        // Gradle's loadCsvSpecData task), nothing has initialized the ES logging system before this class is loaded.
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    private static final Logger logger = LogManager.getLogger(CsvTestsDataLoader.class);

    private static final int BULK_DATA_SIZE = 100_000;

    private static final RequestOptions DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                return false;
            } else {
                for (String warning : warnings) {
                    if ("Parameter [default_metric] is deprecated and will be removed in a future version".equals(warning) == false) {
                        return true;
                    }
                }
                return false;
            }
        })
        .build();

    public static final Map<String, TestDataset> CSV_DATASET_MAP = Stream.of(TestDataset.values())
        .collect(Collectors.toUnmodifiableMap(TestDataset::indexName, v -> v));

    private static final EnrichConfig LANGUAGES_ENRICH = new EnrichConfig("languages_policy", "enrich-policy-languages.json");
    private static final EnrichConfig CLIENT_IPS_ENRICH = new EnrichConfig("clientip_policy", "enrich-policy-clientips.json");
    private static final EnrichConfig CLIENT_CIDR_ENRICH = new EnrichConfig("client_cidr_policy", "enrich-policy-client_cidr.json");
    private static final EnrichConfig AGES_ENRICH = new EnrichConfig("ages_policy", "enrich-policy-ages.json");
    private static final EnrichConfig HEIGHTS_ENRICH = new EnrichConfig("heights_policy", "enrich-policy-heights.json");
    private static final EnrichConfig DECADES_ENRICH = new EnrichConfig("decades_policy", "enrich-policy-decades.json");
    private static final EnrichConfig CITY_NAMES_ENRICH = new EnrichConfig("city_names", "enrich-policy-city_names.json");
    private static final EnrichConfig CITY_BOUNDARIES_ENRICH = new EnrichConfig("city_boundaries", "enrich-policy-city_boundaries.json");
    private static final EnrichConfig CITY_AIRPORTS_ENRICH = new EnrichConfig("city_airports", "enrich-policy-city_airports.json");
    private static final EnrichConfig CITY_LOCATIONS_ENRICH = new EnrichConfig("city_locations", "enrich-policy-city_locations.json");
    private static final EnrichConfig COLORS_ENRICH = new EnrichConfig("colors_policy", "enrich-policy-colors_cmyk.json");

    public static final List<String> ENRICH_SOURCE_INDICES = List.of(
        "languages",
        "clientips",
        "client_cidr",
        "ages",
        "heights",
        "decades",
        "airport_city_boundaries",
        "colors_cmyk"
    );
    public static final List<EnrichConfig> ENRICH_POLICIES = List.of(
        LANGUAGES_ENRICH,
        CLIENT_IPS_ENRICH,
        CLIENT_CIDR_ENRICH,
        AGES_ENRICH,
        HEIGHTS_ENRICH,
        DECADES_ENRICH,
        CITY_NAMES_ENRICH,
        CITY_BOUNDARIES_ENRICH,
        CITY_AIRPORTS_ENRICH,
        CITY_LOCATIONS_ENRICH,
        COLORS_ENRICH
    );
    public static final String NUMERIC_REGEX = "-?\\d+(\\.\\d+)?";

    private static final ViewConfig COUNTRY_ADDRESSES = new ViewConfig("country_addresses");
    private static final ViewConfig COUNTRY_AIRPORTS = new ViewConfig("country_airports");
    private static final ViewConfig COUNTRY_LANGUAGES = new ViewConfig("country_languages");
    private static final ViewConfig AIRPORTS_MP_FILTERED = new ViewConfig("airports_mp_filtered");
    private static final ViewConfig EMPLOYEES_REHIRED = new ViewConfig("employees_rehired");
    private static final ViewConfig EMPLOYEES_NOT_REHIRED = new ViewConfig("employees_not_rehired");
    public static final List<ViewConfig> VIEW_CONFIGS = List.of(
        COUNTRY_ADDRESSES,
        COUNTRY_AIRPORTS,
        COUNTRY_LANGUAGES,
        AIRPORTS_MP_FILTERED,
        EMPLOYEES_REHIRED,
        EMPLOYEES_NOT_REHIRED
    );

    /**
     * <p>
     * Loads spec data on a local ES server.
     * </p>
     * <p>
     * Accepts an URL as first argument, eg. http://localhost:9200 or http://user:pass@localhost:9200
     *</p>
     * <p>
     * If no arguments are specified, the default URL is http://localhost:9200 without authentication
     * </p>
     * <p>
     * It also supports HTTPS
     * </p>
     * @param args the URL to connect
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // Need to setup the log configuration properly to avoid messages when creating a new RestClient
        PluginManager.addPackage(LogConfigurator.class.getPackage().getName());
        LogConfigurator.configureESLogging();
        boolean indexes = false;
        boolean policies = false;
        boolean views = false;
        boolean delete = false;
        boolean load = false;

        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        for (String arg : args) {
            if (arg.startsWith("--")) {
                switch (arg.substring(2).toLowerCase(Locale.ROOT)) {
                    case "indexes", "indices", "data":
                        indexes = true;
                        break;
                    case "policies", "enrich":
                        policies = true;
                        break;
                    case "views":
                        views = true;
                        break;
                    case "delete":
                        delete = true;
                        break;
                    case "load":
                        load = true;
                        break;
                    default:
                        throw new IllegalArgumentException(
                            "unknown option: " + arg + " (valid options are: --indexes, --policies, --views, --delete, --load)"
                        );
                }
            } else {
                URL url = URI.create(args[0]).toURL();
                String protocol = url.getProtocol();
                String host = url.getHost();
                int port = url.getPort();
                if (port < 0 || port > 65535) {
                    throw new IllegalArgumentException("Please specify a valid port [0 - 65535], found [" + port + "]");
                }
                builder = RestClient.builder(new HttpHost(host, port, protocol));
                String userInfo = url.getUserInfo();
                if (userInfo != null) {
                    if (userInfo.contains(":") == false || userInfo.split(":").length != 2) {
                        throw new IllegalArgumentException("Invalid user credentials [username:password], found [" + userInfo + "]");
                    }
                    String[] userPw = userInfo.split(":");
                    String username = userPw[0];
                    String password = userPw[1];
                    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                    builder = builder.setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    );
                }
            }
        }
        // Choose all if none specified
        if (indexes == false && policies == false && views == false) {
            indexes = true;
            policies = true;
            views = true;
        }
        // Delete and re-load if none specified
        if (delete == false && load == false) {
            delete = true;
            load = true;
        }

        try (RestClient client = builder.build()) {
            if (delete) {
                if (views) {
                    deleteViews(client);
                }
                if (policies) {
                    deleteEnrichPolicies(client);
                }
                if (indexes) {
                    deleteIndexes(client, true, true, false, false, cap -> true);
                }
            }
            if (load) {
                if (indexes) {
                    loadDataSets(client, true, true, false, false, cap -> true, (restClient, indexName, indexMapping, indexSettings) -> {
                        // don't use ESRestTestCase methods here or, if you do, test running the main method before making the change
                        StringBuilder jsonBody = new StringBuilder("{");
                        if (indexSettings != null && indexSettings.isEmpty() == false) {
                            jsonBody.append("\"settings\":");
                            jsonBody.append(Strings.toString(indexSettings));
                            jsonBody.append(",");
                        }
                        jsonBody.append("\"mappings\":");
                        jsonBody.append(indexMapping);
                        jsonBody.append("}");

                        Request request = new Request("PUT", "/" + indexName);
                        request.setJsonEntity(jsonBody.toString());
                        restClient.performRequest(request);
                    });
                }
                if (policies) {
                    loadEnrichPolicies(client);
                }
                if (views) {
                    loadViewsIntoEs(client);
                }
            }
        }
    }

    public static Set<TestDataset> availableDatasetsForEs(
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean requiresTimeSeries,
        Predicate<EsqlCapabilities.Cap> capabilityCheck
    ) throws IOException {
        Set<TestDataset> testDataSets = new HashSet<>();

        for (TestDataset dataset : CSV_DATASET_MAP.values()) {
            if ((inferenceEnabled || dataset.requiresInferenceEndpoint == false)
                && (supportsIndexModeLookup || isLookupDataset(dataset) == false)
                && (supportsSourceFieldMapping || isSourceMappingDataset(dataset) == false)
                && (requiresTimeSeries == false || isTimeSeries(dataset))
                && dataset.requiredCapabilities.stream().allMatch(capabilityCheck)) {
                testDataSets.add(dataset);
            }
        }

        return testDataSets;
    }

    private static boolean isLookupDataset(TestDataset dataset) throws IOException {
        Settings settings = dataset.loadSettings();
        String mode = settings.get("index.mode");
        return (mode != null && mode.equalsIgnoreCase("lookup"));
    }

    private static boolean isSourceMappingDataset(TestDataset dataset) throws IOException {
        if (dataset.mappingFileName() == null) {
            return true;
        }
        JsonNode mappingNode = new ObjectMapper().readTree(dataset.streamMapping());
        // BWC tests don't support _source field mappings, so don't load those datasets.
        return mappingNode.get("_source") != null;
    }

    private static boolean isTimeSeries(TestDataset dataset) throws IOException {
        Settings settings = dataset.loadSettings();
        String mode = settings.get("index.mode");
        return (mode != null && mode.equalsIgnoreCase("time_series"));
    }

    public static void loadDataSetIntoEs(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled
    ) throws IOException {
        loadDataSetIntoEs(client, supportsIndexModeLookup, supportsSourceFieldMapping, inferenceEnabled, false, cap -> false);
    }

    public static void loadDataSetIntoEs(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck
    ) throws IOException {
        loadDataSets(
            client,
            supportsIndexModeLookup,
            supportsSourceFieldMapping,
            inferenceEnabled,
            timeSeriesOnly,
            capabilityCheck,
            (restClient, indexName, indexMapping, indexSettings) -> {
                ESRestTestCase.createIndex(
                    restClient,
                    indexName,
                    indexSettings,
                    indexMapping,
                    null,
                    DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER
                );
            }
        );
        if (timeSeriesOnly == false) {
            loadEnrichPolicies(client);
        }
    }

    private static void loadDataSets(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck,
        IndexCreator indexCreator
    ) throws IOException {
        Set<String> loadedDatasets = new HashSet<>();
        logger.info("Loading test datasets");
        for (var dataset : availableDatasetsForEs(
            supportsIndexModeLookup,
            supportsSourceFieldMapping,
            inferenceEnabled,
            timeSeriesOnly,
            capabilityCheck
        )) {
            load(client, dataset, indexCreator);
            loadedDatasets.add(dataset.indexName);
        }
        forceMerge(client, loadedDatasets);
    }

    private static void loadEnrichPolicies(RestClient client) throws IOException {
        logger.info("Loading enrich policies");
        for (var policy : ENRICH_POLICIES) {
            loadEnrichPolicy(client, policy);
        }
    }

    public static void loadViewsIntoEs(RestClient client) throws IOException {
        if (clusterHasViewSupport(client)) {
            logger.info("Loading views");
            for (var view : VIEW_CONFIGS) {
                loadView(client, view);
            }
        } else {
            logger.info("Skipping loading views as the cluster does not support views");
        }
    }

    public static void deleteViews(RestClient client) throws IOException {
        if (clusterHasViewSupport(client)) {
            logger.debug("Deleting views");
            for (var view : VIEW_CONFIGS) {
                deleteView(client, view.name);
            }
        } else {
            logger.info("Skipping deleting views as the cluster does not support views");
        }
    }

    private static void deleteIndexes(
        RestClient client,
        boolean supportsIndexModeLookup,
        boolean supportsSourceFieldMapping,
        boolean inferenceEnabled,
        boolean timeSeriesOnly,
        Predicate<EsqlCapabilities.Cap> capabilityCheck
    ) throws IOException {
        logger.info("Deleting test datasets");
        for (var dataset : availableDatasetsForEs(
            supportsIndexModeLookup,
            supportsSourceFieldMapping,
            inferenceEnabled,
            timeSeriesOnly,
            capabilityCheck
        )) {
            deleteIndex(client, dataset.indexName());
        }
    }

    private static void deleteEnrichPolicies(RestClient client) throws IOException {
        logger.debug("Deleting enrich policies");
        for (var policy : ENRICH_POLICIES) {
            deleteEnrichPolicy(client, policy.policyName);
        }
    }

    public static void createInferenceEndpoints(RestClient client) throws IOException {
        if (clusterHasSparseEmbeddingInferenceEndpoint(client) == false) {
            createSparseEmbeddingInferenceEndpoint(client);
        }

        if (clusterHasTextEmbeddingInferenceEndpoint(client) == false) {
            createTextEmbeddingInferenceEndpoint(client);
        }

        if (clusterHasRerankInferenceEndpoint(client) == false) {
            createRerankInferenceEndpoint(client);
        }

        if (clusterHasCompletionInferenceEndpoint(client) == false) {
            createCompletionInferenceEndpoint(client);
        }
    }

    public static void deleteInferenceEndpoints(RestClient client) throws IOException {
        deleteSparseEmbeddingInferenceEndpoint(client);
        deleteTextEmbeddingInferenceEndpoint(client);
        deleteRerankInferenceEndpoint(client);
        deleteCompletionInferenceEndpoint(client);
    }

    /** The semantic_text mapping type requires inference endpoints that need to be setup before creating the index. */
    public static void createSparseEmbeddingInferenceEndpoint(RestClient client) throws IOException {
        createInferenceEndpoint(client, TaskType.SPARSE_EMBEDDING, "test_sparse_inference", """
                  {
                   "service": "test_service",
                   "service_settings": { "model": "my_model", "api_key": "abc64" },
                   "task_settings": { }
                 }
            """);
    }

    public static void createTextEmbeddingInferenceEndpoint(RestClient client) throws IOException {
        createInferenceEndpoint(client, TaskType.TEXT_EMBEDDING, "test_dense_inference", """
                  {
                   "service": "text_embedding_test_service",
                   "service_settings": {
                        "model": "my_model",
                        "api_key": "abc64",
                        "dimensions": 3,
                        "similarity": "l2_norm",
                        "element_type": "float"
                    },
                   "task_settings": { }
                 }
            """);
    }

    public static void deleteSparseEmbeddingInferenceEndpoint(RestClient client) throws IOException {
        deleteInferenceEndpoint(client, "test_sparse_inference");
    }

    public static void deleteTextEmbeddingInferenceEndpoint(RestClient client) throws IOException {
        deleteInferenceEndpoint(client, "test_dense_inference");
    }

    public static boolean clusterHasSparseEmbeddingInferenceEndpoint(RestClient client) throws IOException {
        return clusterHasInferenceEndpoint(client, TaskType.SPARSE_EMBEDDING, "test_sparse_inference");
    }

    public static boolean clusterHasTextEmbeddingInferenceEndpoint(RestClient client) throws IOException {
        return clusterHasInferenceEndpoint(client, TaskType.TEXT_EMBEDDING, "test_dense_inference");
    }

    public static void createRerankInferenceEndpoint(RestClient client) throws IOException {
        createInferenceEndpoint(client, TaskType.RERANK, "test_reranker", """
            {
                "service": "test_reranking_service",
                "service_settings": { "model_id": "my_model", "api_key": "abc64" },
                "task_settings": { "use_text_length": true }
            }
            """);
    }

    public static void deleteRerankInferenceEndpoint(RestClient client) throws IOException {
        deleteInferenceEndpoint(client, "test_reranker");
    }

    public static boolean clusterHasRerankInferenceEndpoint(RestClient client) throws IOException {
        return clusterHasInferenceEndpoint(client, TaskType.RERANK, "test_reranker");
    }

    public static void createCompletionInferenceEndpoint(RestClient client) throws IOException {
        createInferenceEndpoint(client, TaskType.COMPLETION, "test_completion", """
            {
                "service": "completion_test_service",
                "service_settings": { "model": "my_model", "api_key": "abc64" },
                "task_settings": { "temperature": 3 }
            }
            """);
    }

    public static void deleteCompletionInferenceEndpoint(RestClient client) throws IOException {
        deleteInferenceEndpoint(client, "test_completion");
    }

    public static boolean clusterHasCompletionInferenceEndpoint(RestClient client) throws IOException {
        return clusterHasInferenceEndpoint(client, TaskType.COMPLETION, "test_completion");
    }

    private static void createInferenceEndpoint(RestClient client, TaskType taskType, String inferenceId, String modelSettings)
        throws IOException {
        Request request = new Request("PUT", "/_inference/" + taskType.name() + "/" + inferenceId);
        request.setJsonEntity(modelSettings);
        client.performRequest(request);
    }

    private static boolean clusterHasInferenceEndpoint(RestClient client, TaskType taskType, String inferenceId) throws IOException {
        Request request = new Request("GET", "/_inference/" + taskType.name() + "/" + inferenceId);
        try {
            client.performRequest(request);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
        return true;
    }

    private static void deleteInferenceEndpoint(RestClient client, String inferenceId) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "/_inference/" + inferenceId));
        } catch (ResponseException e) {
            // 404 here means the endpoint was not created
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public static void loadEnrichPolicy(RestClient client, EnrichConfig policy) throws IOException {
        logger.debug("Loading enrich policy [{}]", policy.policyName);
        Request request = new Request("PUT", "/_enrich/policy/" + policy.policyName);
        request.setJsonEntity(policy.loadPolicy());
        client.performRequest(request);

        request = new Request("POST", "/_enrich/policy/" + policy.policyName + "/_execute");
        client.performRequest(request);
    }

    private static void loadView(RestClient client, ViewConfig view) throws IOException {
        logger.debug("Loading view [{}] from file [/views/{}.esql]", view.name, view.name);
        Request request = new Request("PUT", "/_query/view/" + view.name);
        request.setJsonEntity("{\"query\":\"" + view.loadQuery().replace("\"", "\\\"").replace("\n", "") + "\"}");
        client.performRequest(request);
    }

    private static boolean clusterHasViewSupport(RestClient client) throws IOException {
        Request request = new Request("GET", "/_query/view");
        try {
            Response ignored = client.performRequest(request);
        } catch (ResponseException e) {
            int code = e.getResponse().getStatusLine().getStatusCode();
            // Different versions of Elasticsearch return different codes when views are not supported
            if (code == 410 || code == 400 || code == 500 || code == 405) {
                return false;
            }
            throw e;
        }
        return true;
    }

    private static void deleteView(RestClient client, String viewName) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "/_query/view/" + viewName));
        } catch (ResponseException e) {
            int code = e.getResponse().getStatusLine().getStatusCode();
            // On older servers the view listing succeeds when it should not, so we get here when we should not, hence the 400 and 500
            if (code != 404 && code != 400 && code != 410 && code != 500) {
                logger.info("View delete error: {}", e.getMessage());
                throw e;
            }
        }
    }

    private static void deleteIndex(RestClient client, String indexName) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "/" + indexName));
        } catch (ResponseException e) {
            logger.info("Index delete error: {}", e.getMessage());
        }
    }

    private static void deleteEnrichPolicy(RestClient client, String policyName) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "/_enrich/policy/" + policyName));
        } catch (ResponseException e) {
            logger.info("Enrich policy delete error: {}", e.getMessage());
        }
    }

    public static InputStream getResourceStream(String name) {
        return Objects.requireNonNull(CsvTestsDataLoader.class.getResourceAsStream(name), "Cannot find resource " + name);
    }

    public static String getResourceString(String name) {
        try (var stream = getResourceStream(name)) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void load(RestClient client, TestDataset dataset, IndexCreator indexCreator) throws IOException {
        logger.debug("Loading dataset [{}] into ES index [{}]", dataset.dataFileName, dataset.indexName);
        indexCreator.createIndex(client, dataset.indexName, readMappingFile(dataset), dataset.loadSettings());

        // Some examples only test that the query and mappings are valid, and don't need example data. Use .noData() for those
        if (dataset.dataFileName != null) {
            loadCsvData(client, dataset.indexName, dataset.streamData(), dataset.allowSubFields);
        }
    }

    private static String readMappingFile(TestDataset dataset) throws IOException {
        String mappingJsonText = dataset.loadMappings();
        if (dataset.typeMapping == null || dataset.typeMapping.isEmpty()) {
            return mappingJsonText;
        }
        boolean modified = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mappingNode = mapper.readTree(mappingJsonText);
        JsonNode propertiesNode = mappingNode.path("properties");

        for (Map.Entry<String, String> entry : dataset.typeMapping.entrySet()) {
            String key = entry.getKey();
            String newType = entry.getValue();

            if (propertiesNode.has(key)) {
                modified = true;
                ((ObjectNode) propertiesNode.get(key)).put("type", newType);
            }
        }
        if (modified) {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mappingNode);
        }
        return mappingJsonText;
    }

    record ColumnHeader(String name, String type) {}

    /**
     * Loads a classic csv file in an ES cluster using a RestClient.
     * The structure of the file is as follows:
     * - commented lines should start with "//"
     * - the first non-comment line from the file is the schema line (comma separated field_name:ES_data_type elements)
     *   - sub-fields should be placed after the root field using a dot notation for the name:
     *       root_field:long,root_field.sub_field:integer
     *   - a special _id field can be used in the schema and the values of this field will be used in the bulk request as actual doc ids
     * - all subsequent non-comment lines represent the values that will be used to build the _bulk request
     * - an empty string "" refers to a null value
     * - a value starting with an opening square bracket "[" and ending with a closing square bracket "]" refers to a multi-value field
     *   - multi-values are comma separated
     *   - commas inside multivalue fields can be escaped with \ (backslash) character
     */
    public static void loadCsvData(RestClient client, String indexName, InputStream resource, boolean allowSubFields) throws IOException {

        ArrayList<String> failures = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = reader(resource)) {
            String line;
            int lineNumber = 1;
            ColumnHeader[] columns = null; // Column info. If one column name contains dot, it is a subfield and its value will be null
            List<Integer> subFieldsIndices = new ArrayList<>(); // list containing the index of a subfield in "columns" String[]

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (line.isEmpty() == false && line.startsWith("//") == false) {
                    String[] entries = multiValuesAwareCsvToStringArray(line, lineNumber);
                    // the schema row
                    if (columns == null) {
                        columns = new ColumnHeader[entries.length];
                        for (int i = 0; i < entries.length; i++) {
                            int split = entries[i].indexOf(':');
                            if (split < 0) {
                                columns[i] = new ColumnHeader(entries[i].trim(), null);
                            } else {
                                String name = entries[i].substring(0, split).trim();
                                String type = entries[i].substring(split + 1).trim();
                                if (allowSubFields || name.contains(".") == false) {
                                    columns[i] = new ColumnHeader(name, type);
                                } else {// if it's a subfield, ignore it in the _bulk request
                                    columns[i] = null;
                                    subFieldsIndices.add(i);
                                }
                            }
                        }
                    }
                    // data rows
                    else {
                        if (entries.length != columns.length) {
                            throw new IllegalArgumentException(
                                format(
                                    null,
                                    "Error line [{}]: Incorrect number of entries; expected [{}] but found [{}]",
                                    lineNumber,
                                    columns.length,
                                    entries.length
                                )
                            );
                        }
                        StringBuilder row = new StringBuilder();
                        String idField = null;
                        for (int i = 0; i < entries.length; i++) {
                            // ignore values that belong to subfields and don't add them to the bulk request
                            if (subFieldsIndices.contains(i) == false) {
                                if ("".equals(entries[i])) {
                                    // Value is null, skip
                                    continue;
                                }
                                if (columns[i] != null && "_id".equals(columns[i].name)) {
                                    // Value is an _id
                                    idField = entries[i];
                                    continue;
                                }
                                try {
                                    // add a comma after the previous value, only when there was actually a value before
                                    if (i > 0 && row.length() > 0) {
                                        row.append(",");
                                    }
                                    // split on comma ignoring escaped commas
                                    String[] multiValues = entries[i].split(COMMA_ESCAPING_REGEX);
                                    if (multiValues.length > 1) {
                                        StringBuilder rowStringValue = new StringBuilder("[");
                                        for (String s : multiValues) {
                                            rowStringValue.append(toJson(columns[i].type, s)).append(",");
                                        }
                                        // remove the last comma and put a closing bracket instead
                                        rowStringValue.replace(rowStringValue.length() - 1, rowStringValue.length(), "]");
                                        entries[i] = rowStringValue.toString();
                                    } else {
                                        entries[i] = toJson(columns[i].type, entries[i]);
                                    }
                                    // replace any escaped commas with single comma
                                    entries[i] = entries[i].replace(ESCAPED_COMMA_SEQUENCE, ",");
                                    row.append("\"").append(columns[i].name).append("\":").append(entries[i]);
                                } catch (Exception e) {
                                    throw new IllegalArgumentException(
                                        format(
                                            null,
                                            "Error line [{}]: Cannot parse entry [{}] with value [{}]",
                                            lineNumber,
                                            i + 1,
                                            entries[i]
                                        ),
                                        e
                                    );
                                }
                            }
                        }
                        String idPart = idField != null ? "\", \"_id\": \"" + idField : "";
                        builder.append("{\"index\": {\"_index\":\"" + indexName + idPart + "\"}}\n");
                        builder.append("{" + row + "}\n");
                    }
                }
                lineNumber++;
                if (builder.length() > BULK_DATA_SIZE) {
                    sendBulkRequest(indexName, builder, client, failures);
                    builder.setLength(0);
                }
            }
        }
        if (builder.isEmpty() == false) {
            sendBulkRequest(indexName, builder, client, failures);
        }
        if (failures.isEmpty() == false) {
            for (String failure : failures) {
                logger.error(failure);
            }
            throw new IOException("Data loading failed with " + failures.size() + " errors: " + failures.get(0));
        }
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("([0-9\\-.Z:]+)\\.\\.([0-9\\-.Z:]+)");

    private static String toJson(String type, String value) {
        return switch (type == null ? "" : type) {
            case "date_range", "double_range", "integer_range" -> {
                Matcher m = RANGE_PATTERN.matcher(value);
                if (m.matches() == false) {
                    throw new IllegalArgumentException("can't parse range: " + value);
                }
                yield "{\"gte\": \"" + m.group(1) + "\", \"lt\": \"" + m.group(2) + "\"}";
            }
            default -> {
                boolean isQuoted = (value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("{") && value.endsWith("}"));
                boolean isNumeric = value.matches(NUMERIC_REGEX);
                yield isQuoted || isNumeric ? value : "\"" + value + "\"";
            }
        };
    }

    private static void sendBulkRequest(String indexName, StringBuilder builder, RestClient client, List<String> failures)
        throws IOException {
        // The indexName is optional for a bulk request, but we use it for routing in MultiClusterSpecIT.
        builder.append("\n");
        logger.trace("Sending bulk request of [{}] bytes for [{}]", builder.length(), indexName);
        Request request = new Request("POST", "/" + indexName + "/_bulk");
        request.setJsonEntity(builder.toString());
        request.addParameter("refresh", "false"); // will be _forcemerge'd next
        Response response = client.performRequest(request);
        if (response.getStatusLine().getStatusCode() == 200) {
            HttpEntity entity = response.getEntity();
            try (InputStream content = entity.getContent()) {
                XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
                Map<String, Object> result = XContentHelper.convertToMap(xContentType.xContent(), content, false);
                Object errors = result.get("errors");
                if (Boolean.TRUE.equals(errors)) {
                    addError(failures, indexName, builder, "errors: " + result);
                }
            }
        } else {
            addError(failures, indexName, builder, "status: " + response.getStatusLine());
        }
    }

    private static void addError(List<String> failures, String indexName, StringBuilder builder, String message) {
        failures.add(
            format(
                "Data loading of [{}] bytes into [{}] failed with {}: Data [{}...]",
                builder.length(),
                indexName,
                message,
                builder.substring(0, 100)
            )
        );
    }

    private static void forceMerge(RestClient client, Set<String> indices) throws IOException {
        String pattern = String.join(",", indices);

        Request request = new Request("POST", "/" + pattern + "/_forcemerge?max_num_segments=1");
        Response response = client.performRequest(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            logger.warn("Force-merging [{}] to 1 segment failed: {}", pattern, response.getStatusLine());
        } else {
            logger.info("[{}] forced-merged to 1 segment", pattern);
        }
    }

    public record MultiIndexTestDataset(String indexPattern, List<TestDataset> datasets) {
        public static MultiIndexTestDataset of(TestDataset testsDataset) {
            return new MultiIndexTestDataset(testsDataset.indexName, List.of(testsDataset));
        }
    }

    public enum TestDataset {
        EMPLOYEES("employees", "mapping-default.json", "employees.csv", null, false, null, null, false, List.of()),
        VOYAGER("voyager", "mapping-voyager.json", "voyager.csv", null, false, null, null, false, List.of()),
        EMPLOYEES_INCOMPATIBLE(
            "employees_incompatible",
            "mapping-default-incompatible.json",
            "employees_incompatible.csv",
            null,
            false,
            null,
            null,
            false,
            List.of()
        ),
        ALL_TYPES("all_types", "mapping-all-types.json", "all-types.csv"),
        HOSTS("hosts"),
        APPS("apps"),
        APPS_SHORT("apps_short", "mapping-apps.json", "apps.csv", null, true, Map.of("id", "short"), null, false, List.of()),
        MULTI_COLUMN_JOINABLE("multi_column_joinable", "mapping-multi_column_joinable.json", "multi_column_joinable.csv"),
        MULTI_COLUMN_JOINABLE_LOOKUP(
            "multi_column_joinable_lookup",
            "mapping-multi_column_joinable_lookup.json",
            "multi_column_joinable_lookup.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        LANGUAGES("languages"),
        LANGUAGES_LOOKUP(
            "languages_lookup",
            "mapping-languages.json",
            "languages.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        LANGUAGES_NON_UNIQUE_KEY("languages_non_unique_key"),
        LANGUAGES_LOOKUP_NON_UNIQUE_KEY(
            "languages_lookup_non_unique_key",
            "mapping-languages.json",
            "languages_non_unique_key.csv",
            "lookup-settings.json",
            true,
            null,
            Map.of("country", "text"),
            false,
            List.of()
        ),
        LANGUAGES_NESTED_FIELDS(
            "languages_nested_fields",
            "mapping-languages_nested_fields.json",
            "languages_nested_fields.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        LANGUAGES_MIX_NUMERICS(
            "languages_mixed_numerics",
            "mapping-languages_mixed_numerics.json",
            "languages_mixed_numerics.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        ALERTS("alerts"),
        UL_LOGS("ul_logs"),
        SAMPLE_DATA("sample_data"),
        MV_SAMPLE_DATA("mv_sample_data"),
        SAMPLE_DATA_STR(
            "sample_data_str",
            "mapping-sample_data.json",
            "sample_data.csv",
            null,
            true,
            Map.of("client_ip", "keyword"),
            null,
            false,
            List.of()
        ),
        SAMPLE_DATA_TS_LONG(
            "sample_data_ts_long",
            "mapping-sample_data.json",
            "sample_data_ts_long.csv",
            null,
            true,
            Map.of("@timestamp", "long"),
            null,
            false,
            List.of()
        ),
        SAMPLE_DATA_TS_NANOS(
            "sample_data_ts_nanos",
            "mapping-sample_data.json",
            "sample_data_ts_nanos.csv",
            null,
            true,
            Map.of("@timestamp", "date_nanos"),
            null,
            false,
            List.of()
        ),
        // the double underscore is meant to not match `sample_data*`, but do match `sample_*`
        SAMPLE_DATA_TS_NANOS_LOOKUP(
            "sample__data_ts_nanos_lookup",
            "mapping-sample_data.json",
            "sample_data_ts_nanos.csv",
            "lookup-settings.json",
            true,
            Map.of("@timestamp", "date_nanos"),
            null,
            false,
            List.of()
        ),
        MISSING_IP_SAMPLE_DATA("missing_ip_sample_data"),
        SAMPLE_DATA_PARTIAL_MAPPING("partial_mapping_sample_data"),
        SAMPLE_DATA_NO_MAPPING(
            "no_mapping_sample_data",
            "mapping-no_mapping_sample_data.json",
            "partial_mapping_sample_data.csv",
            null,
            true,
            Map.of("timestamp", "keyword", "client_ip", "keyword", "event_duration", "keyword"),
            null,
            false,
            List.of()
        ),
        SAMPLE_DATA_PARTIAL_MAPPING_NO_SOURCE(
            "partial_mapping_no_source_sample_data",
            "mapping-partial_mapping_no_source_sample_data.json",
            "partial_mapping_sample_data.csv"
        ),
        SAMPLE_DATA_PARTIAL_MAPPING_EXCLUDED_SOURCE(
            "partial_mapping_excluded_source_sample_data",
            "mapping-partial_mapping_excluded_source_sample_data.json",
            "partial_mapping_sample_data.csv"
        ),
        CLIENT_IPS("clientips"),
        CLIENT_IPS_LOOKUP(
            "clientips_lookup",
            "mapping-clientips.json",
            "clientips.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        MESSAGE_TYPES("message_types"),
        MESSAGE_TYPES_LOOKUP(
            "message_types_lookup",
            "mapping-message_types.json",
            "message_types.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        FIREWALL_LOGS("firewall_logs", "mapping-firewall_logs.json", null),
        THREAT_LIST("threat_list", "mapping-threat_list.json", null, "lookup-settings.json", true, null, null, false, List.of()),
        APP_LOGS("app_logs", "mapping-app_logs.json", null),
        SERVICE_OWNERS("service_owners", "mapping-service_owners.json", null, "lookup-settings.json", true, null, null, false, List.of()),
        SYSTEM_METRICS("system_metrics", "mapping-system_metrics.json", null),
        HOST_INVENTORY("host_inventory", "mapping-host_inventory.json", null, "lookup-settings.json", true, null, null, false, List.of()),
        OWNERSHIPS("ownerships", "mapping-ownerships.json", null, "lookup-settings.json", true, null, null, false, List.of()),
        CLIENT_CIDR("client_cidr"),
        AGES("ages"),
        HEIGHTS("heights"),
        DECADES("decades"),
        AIRPORTS("airports"),
        AIRPORTS_MP("airports_mp", "mapping-airports.json", "airports_mp.csv", "lookup-settings.json", true, null, null, false, List.of()),
        AIRPORTS_NO_DOC_VALUES("airports_no_doc_values", "mapping-airports_no_doc_values.json", "airports.csv"),
        AIRPORTS_NOT_INDEXED("airports_not_indexed", "mapping-airports_not_indexed.json", "airports.csv"),
        AIRPORTS_NOT_INDEXED_NOR_DOC_VALUES(
            "airports_not_indexed_nor_doc_values",
            "mapping-airports_not_indexed_nor_doc_values.json",
            "airports.csv"
        ),
        AIRPORTS_WEB("airports_web"),
        DATE_NANOS("date_nanos"),
        DATE_NANOS_UNION_TYPES("date_nanos_union_types"),
        COUNTRIES_BBOX("countries_bbox"),
        COUNTRIES_BBOX_WEB("countries_bbox_web"),
        AIRPORT_CITY_BOUNDARIES(
            "airport_city_boundaries",
            "mapping-airport_city_boundaries.json",
            "airport_city_boundaries.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        CARTESIAN_MULTIPOLYGONS("cartesian_multipolygons"),
        CARTESIAN_MULTIPOLYGONS_NO_DOC_VALUES(
            "cartesian_multipolygons_no_doc_values",
            "mapping-cartesian_multipolygons_no_doc_values.json",
            "cartesian_multipolygons.csv"
        ),
        MULTIVALUE_GEOMETRIES("multivalue_geometries"),
        MULTIVALUE_POINTS("multivalue_points"),
        DISTANCES("distances"),
        K8S("k8s", "k8s-mappings.json", "k8s.csv", "k8s-settings.json", true, null, null, false, List.of()),
        K8S_DATENANOS("datenanos-k8s", "k8s-mappings-date_nanos.json", "k8s.csv", "k8s-settings.json", true, null, null, false, List.of()),
        K8S_DOWNSAMPLED(
            "k8s-downsampled",
            "k8s-downsampled-mappings.json",
            "k8s-downsampled.csv",
            "k8s-downsampled-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        ADDRESSES("addresses"),
        BOOKS("books", "mapping-books.json", "books.csv", "books-settings.json", true, null, null, false, List.of()),
        SEMANTIC_TEXT("semantic_text", "mapping-semantic_text.json", "semantic_text.csv", null, true, null, null, true, List.of()),
        LOGS("logs"),
        DENSE_VECTOR_TEXT("dense_vector_text"),
        MV_TEXT("mv_text"),
        DENSE_VECTOR("dense_vector"),
        DENSE_VECTOR_BFLOAT16(
            "dense_vector_bfloat16",
            "mapping-dense_vector_bfloat16.json",
            "dense_vector_bfloat16.csv",
            null,
            true,
            null,
            null,
            false,
            List.of(EsqlCapabilities.Cap.GENERIC_VECTOR_FORMAT)
        ),
        DENSE_VECTOR_ARITHMETIC("dense_vector_arithmetic"),
        WEB_LOGS("web_logs"),
        COLORS("colors"),
        COLORS_CMYK_LOOKUP(
            "colors_cmyk",
            "mapping-colors_cmyk.json",
            "colors_cmyk.csv",
            "lookup-settings.json",
            true,
            null,
            null,
            false,
            List.of()
        ),
        BASE_CONVERSION("base_conversion"),
        EXP_HISTO_SAMPLE(
            "exp_histo_sample",
            "exp_histo_sample-mappings.json",
            "exp_histo_sample.csv",
            "exp_histo_sample-settings.json",
            true,
            null,
            null,
            false,
            List.of(EsqlCapabilities.Cap.EXPONENTIAL_HISTOGRAM_TECH_PREVIEW)
        ),
        TDIGEST_STANDARD_INDEX(
            "tdigest_standard_index",
            "mapping-tdigest_standard_index.json",
            "tdigest_standard_index.csv",
            null,
            true,
            null,
            null,
            false,
            List.of(EsqlCapabilities.Cap.TDIGEST_TECH_PREVIEW)
        ),
        HISTOGRAM_STANDARD_INDEX(
            "histogram_standard_index",
            "mapping-histogram_standard_index.json",
            "histogram_standard_index.csv",
            null,
            true,
            null,
            null,
            false,
            List.of(EsqlCapabilities.Cap.HISTOGRAM_RELEASE_VERSION)
        ),
        TDIGEST_TIMESERIES_INDEX(
            "tdigest_timeseries_index",
            "tdigest_timeseries_index-mappings.json",
            "tdigest_standard_index.csv",
            "tdigest_timeseries_index-settings.json",
            true,
            null,
            null,
            false,
            List.of(EsqlCapabilities.Cap.TDIGEST_TECH_PREVIEW, EsqlCapabilities.Cap.TDIGEST_TIME_SERIES_METRIC)
        ),
        HISTOGRAM_TIMESERIES_INDEX(
            "histogram_timeseries_index",
            "mapping-histogram_time_series_index.json",
            "histogram_standard_index.csv",
            "settings-histogram_time_series_index.json",
            true,
            null,
            null,
            false,
            List.of(EsqlCapabilities.Cap.HISTOGRAM_RELEASE_VERSION)
        ),
        MANY_NUMBERS("many_numbers"),
        MMR_TEXT_VECTOR_KEYWORD("mmr_text_vector_keyword");

        private final String indexName;
        private final String mappingFileName;
        private final String dataFileName;
        private final String settingFileName;
        private final boolean allowSubFields;
        @Nullable
        private final Map<String, String> typeMapping;
        @Nullable
        private final Map<String, String> dynamicTypeMapping;
        private final boolean requiresInferenceEndpoint;
        private final List<EsqlCapabilities.Cap> requiredCapabilities;

        TestDataset(String indexName) {
            this(indexName, "mapping-" + indexName + ".json", indexName + ".csv", null, true, null, null, false, List.of());
        }

        TestDataset(String indexName, String mappingFileName, String dataFileName) {
            this(indexName, mappingFileName, dataFileName, null, true, null, null, false, List.of());
        }

        TestDataset(
            String indexName,
            String mappingFileName,
            String dataFileName,
            String settingFileName,
            boolean allowSubFields,
            @Nullable Map<String, String> typeMapping,
            @Nullable Map<String, String> dynamicTypeMapping,
            boolean requiresInferenceEndpoint,
            List<EsqlCapabilities.Cap> requiredCapabilities
        ) {
            this.indexName = indexName;
            this.mappingFileName = mappingFileName;
            this.dataFileName = dataFileName;
            this.settingFileName = settingFileName;
            this.allowSubFields = allowSubFields;
            this.typeMapping = typeMapping;
            this.dynamicTypeMapping = dynamicTypeMapping;
            this.requiresInferenceEndpoint = requiresInferenceEndpoint;
            this.requiredCapabilities = requiredCapabilities;
        }

        public String indexName() {
            return indexName;
        }

        public String mappingFileName() {
            return mappingFileName;
        }

        public String dataFileName() {
            return dataFileName;
        }

        public String settingFileName() {
            return settingFileName;
        }

        public boolean allowSubFields() {
            return allowSubFields;
        }

        public Map<String, String> typeMapping() {
            return typeMapping;
        }

        public Map<String, String> dynamicTypeMapping() {
            return dynamicTypeMapping;
        }

        public boolean requiresInferenceEndpoint() {
            return requiresInferenceEndpoint;
        }

        public List<EsqlCapabilities.Cap> requiredCapabilities() {
            return requiredCapabilities;
        }

        public Settings loadSettings() throws IOException {
            if (settingFileName == null) {
                return Settings.EMPTY;
            }
            final String settingName = "/index/settings/" + settingFileName;
            return Settings.builder().loadFromStream(settingName, getResourceStream(settingName), false).build();
        }

        public String loadMappings() {
            return getResourceString("/index/mappings/" + mappingFileName);
        }

        public InputStream streamMapping() {
            return getResourceStream("/index/mappings/" + mappingFileName);
        }

        public InputStream streamData() {
            return getResourceStream("/data/" + dataFileName);
        }
    }

    public record ViewConfig(String name) {
        public String loadQuery() {
            return getResourceString("/views/" + name + ".esql");
        }
    }

    public record EnrichConfig(String policyName, String policyFileName) {
        public String loadPolicy() {
            return getResourceString("/enrich/policy/" + policyFileName);
        }

        public InputStream streamPolicy() {
            return getResourceStream("/enrich/policy/" + policyFileName);
        }
    }

    private interface IndexCreator {
        void createIndex(RestClient client, String indexName, String mapping, Settings indexSettings) throws IOException;
    }
}
