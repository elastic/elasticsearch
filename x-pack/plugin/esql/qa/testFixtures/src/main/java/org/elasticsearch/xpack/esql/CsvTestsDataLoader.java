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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.CsvTestUtils.COMMA_ESCAPING_REGEX;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ESCAPED_COMMA_SEQUENCE;
import static org.elasticsearch.xpack.esql.CsvTestUtils.multiValuesAwareCsvToStringArray;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.reader;

public class CsvTestsDataLoader {
    private static final int BULK_DATA_SIZE = 100_000;
    private static final TestDataset EMPLOYEES = new TestDataset("employees", "mapping-default.json", "employees.csv").noSubfields();
    private static final TestDataset EMPLOYEES_INCOMPATIBLE = new TestDataset(
        "employees_incompatible",
        "mapping-default-incompatible.json",
        "employees_incompatible.csv"
    ).noSubfields();
    private static final TestDataset HOSTS = new TestDataset("hosts");
    private static final TestDataset APPS = new TestDataset("apps");
    private static final TestDataset APPS_SHORT = APPS.withIndex("apps_short").withTypeMapping(Map.of("id", "short"));
    private static final TestDataset LANGUAGES = new TestDataset("languages");
    private static final TestDataset LANGUAGES_LOOKUP = LANGUAGES.withIndex("languages_lookup").withSetting("lookup-settings.json");
    private static final TestDataset LANGUAGES_LOOKUP_NON_UNIQUE_KEY = LANGUAGES_LOOKUP.withIndex("languages_lookup_non_unique_key")
        .withData("languages_non_unique_key.csv");
    private static final TestDataset LANGUAGES_NESTED_FIELDS = new TestDataset(
        "languages_nested_fields",
        "mapping-languages_nested_fields.json",
        "languages_nested_fields.csv"
    ).withSetting("lookup-settings.json");
    private static final TestDataset ALERTS = new TestDataset("alerts");
    private static final TestDataset UL_LOGS = new TestDataset("ul_logs");
    private static final TestDataset SAMPLE_DATA = new TestDataset("sample_data");
    private static final TestDataset MV_SAMPLE_DATA = new TestDataset("mv_sample_data");
    private static final TestDataset SAMPLE_DATA_STR = SAMPLE_DATA.withIndex("sample_data_str")
        .withTypeMapping(Map.of("client_ip", "keyword"));
    private static final TestDataset SAMPLE_DATA_TS_LONG = SAMPLE_DATA.withIndex("sample_data_ts_long")
        .withData("sample_data_ts_long.csv")
        .withTypeMapping(Map.of("@timestamp", "long"));
    private static final TestDataset SAMPLE_DATA_TS_NANOS = SAMPLE_DATA.withIndex("sample_data_ts_nanos")
        .withData("sample_data_ts_nanos.csv")
        .withTypeMapping(Map.of("@timestamp", "date_nanos"));
    private static final TestDataset MISSING_IP_SAMPLE_DATA = new TestDataset("missing_ip_sample_data");
    private static final TestDataset CLIENT_IPS = new TestDataset("clientips");
    private static final TestDataset CLIENT_IPS_LOOKUP = CLIENT_IPS.withIndex("clientips_lookup").withSetting("lookup-settings.json");
    private static final TestDataset MESSAGE_TYPES = new TestDataset("message_types");
    private static final TestDataset MESSAGE_TYPES_LOOKUP = MESSAGE_TYPES.withIndex("message_types_lookup")
        .withSetting("lookup-settings.json");
    private static final TestDataset CLIENT_CIDR = new TestDataset("client_cidr");
    private static final TestDataset AGES = new TestDataset("ages");
    private static final TestDataset HEIGHTS = new TestDataset("heights");
    private static final TestDataset DECADES = new TestDataset("decades");
    private static final TestDataset AIRPORTS = new TestDataset("airports");
    private static final TestDataset AIRPORTS_MP = AIRPORTS.withIndex("airports_mp").withData("airports_mp.csv");
    private static final TestDataset AIRPORTS_NO_DOC_VALUES = new TestDataset("airports_no_doc_values").withData("airports.csv");
    private static final TestDataset AIRPORTS_NOT_INDEXED = new TestDataset("airports_not_indexed").withData("airports.csv");
    private static final TestDataset AIRPORTS_NOT_INDEXED_NOR_DOC_VALUES = new TestDataset("airports_not_indexed_nor_doc_values").withData(
        "airports.csv"
    );
    private static final TestDataset AIRPORTS_WEB = new TestDataset("airports_web");
    private static final TestDataset DATE_NANOS = new TestDataset("date_nanos");
    private static final TestDataset COUNTRIES_BBOX = new TestDataset("countries_bbox");
    private static final TestDataset COUNTRIES_BBOX_WEB = new TestDataset("countries_bbox_web");
    private static final TestDataset AIRPORT_CITY_BOUNDARIES = new TestDataset("airport_city_boundaries");
    private static final TestDataset CARTESIAN_MULTIPOLYGONS = new TestDataset("cartesian_multipolygons");
    private static final TestDataset CARTESIAN_MULTIPOLYGONS_NO_DOC_VALUES = new TestDataset("cartesian_multipolygons_no_doc_values")
        .withData("cartesian_multipolygons.csv");
    private static final TestDataset MULTIVALUE_GEOMETRIES = new TestDataset("multivalue_geometries");
    private static final TestDataset MULTIVALUE_POINTS = new TestDataset("multivalue_points");
    private static final TestDataset DISTANCES = new TestDataset("distances");
    private static final TestDataset K8S = new TestDataset("k8s", "k8s-mappings.json", "k8s.csv").withSetting("k8s-settings.json");
    private static final TestDataset ADDRESSES = new TestDataset("addresses");
    private static final TestDataset BOOKS = new TestDataset("books").withSetting("books-settings.json");
    private static final TestDataset SEMANTIC_TEXT = new TestDataset("semantic_text").withInferenceEndpoint(true);

    public static final Map<String, TestDataset> CSV_DATASET_MAP = Map.ofEntries(
        Map.entry(EMPLOYEES.indexName, EMPLOYEES),
        Map.entry(EMPLOYEES_INCOMPATIBLE.indexName, EMPLOYEES_INCOMPATIBLE),
        Map.entry(HOSTS.indexName, HOSTS),
        Map.entry(APPS.indexName, APPS),
        Map.entry(APPS_SHORT.indexName, APPS_SHORT),
        Map.entry(LANGUAGES.indexName, LANGUAGES),
        Map.entry(LANGUAGES_LOOKUP.indexName, LANGUAGES_LOOKUP),
        Map.entry(LANGUAGES_LOOKUP_NON_UNIQUE_KEY.indexName, LANGUAGES_LOOKUP_NON_UNIQUE_KEY),
        Map.entry(LANGUAGES_NESTED_FIELDS.indexName, LANGUAGES_NESTED_FIELDS),
        Map.entry(UL_LOGS.indexName, UL_LOGS),
        Map.entry(SAMPLE_DATA.indexName, SAMPLE_DATA),
        Map.entry(MV_SAMPLE_DATA.indexName, MV_SAMPLE_DATA),
        Map.entry(ALERTS.indexName, ALERTS),
        Map.entry(SAMPLE_DATA_STR.indexName, SAMPLE_DATA_STR),
        Map.entry(SAMPLE_DATA_TS_LONG.indexName, SAMPLE_DATA_TS_LONG),
        Map.entry(SAMPLE_DATA_TS_NANOS.indexName, SAMPLE_DATA_TS_NANOS),
        Map.entry(MISSING_IP_SAMPLE_DATA.indexName, MISSING_IP_SAMPLE_DATA),
        Map.entry(CLIENT_IPS.indexName, CLIENT_IPS),
        Map.entry(CLIENT_IPS_LOOKUP.indexName, CLIENT_IPS_LOOKUP),
        Map.entry(MESSAGE_TYPES.indexName, MESSAGE_TYPES),
        Map.entry(MESSAGE_TYPES_LOOKUP.indexName, MESSAGE_TYPES_LOOKUP),
        Map.entry(CLIENT_CIDR.indexName, CLIENT_CIDR),
        Map.entry(AGES.indexName, AGES),
        Map.entry(HEIGHTS.indexName, HEIGHTS),
        Map.entry(DECADES.indexName, DECADES),
        Map.entry(AIRPORTS.indexName, AIRPORTS),
        Map.entry(AIRPORTS_MP.indexName, AIRPORTS_MP),
        Map.entry(AIRPORTS_NO_DOC_VALUES.indexName, AIRPORTS_NO_DOC_VALUES),
        Map.entry(AIRPORTS_NOT_INDEXED.indexName, AIRPORTS_NOT_INDEXED),
        Map.entry(AIRPORTS_NOT_INDEXED_NOR_DOC_VALUES.indexName, AIRPORTS_NOT_INDEXED_NOR_DOC_VALUES),
        Map.entry(AIRPORTS_WEB.indexName, AIRPORTS_WEB),
        Map.entry(COUNTRIES_BBOX.indexName, COUNTRIES_BBOX),
        Map.entry(COUNTRIES_BBOX_WEB.indexName, COUNTRIES_BBOX_WEB),
        Map.entry(AIRPORT_CITY_BOUNDARIES.indexName, AIRPORT_CITY_BOUNDARIES),
        Map.entry(CARTESIAN_MULTIPOLYGONS.indexName, CARTESIAN_MULTIPOLYGONS),
        Map.entry(CARTESIAN_MULTIPOLYGONS_NO_DOC_VALUES.indexName, CARTESIAN_MULTIPOLYGONS_NO_DOC_VALUES),
        Map.entry(MULTIVALUE_GEOMETRIES.indexName, MULTIVALUE_GEOMETRIES),
        Map.entry(MULTIVALUE_POINTS.indexName, MULTIVALUE_POINTS),
        Map.entry(DATE_NANOS.indexName, DATE_NANOS),
        Map.entry(K8S.indexName, K8S),
        Map.entry(DISTANCES.indexName, DISTANCES),
        Map.entry(ADDRESSES.indexName, ADDRESSES),
        Map.entry(BOOKS.indexName, BOOKS),
        Map.entry(SEMANTIC_TEXT.indexName, SEMANTIC_TEXT)
    );

    private static final EnrichConfig LANGUAGES_ENRICH = new EnrichConfig("languages_policy", "enrich-policy-languages.json");
    private static final EnrichConfig CLIENT_IPS_ENRICH = new EnrichConfig("clientip_policy", "enrich-policy-clientips.json");
    private static final EnrichConfig CLIENT_CIDR_ENRICH = new EnrichConfig("client_cidr_policy", "enrich-policy-client_cidr.json");
    private static final EnrichConfig AGES_ENRICH = new EnrichConfig("ages_policy", "enrich-policy-ages.json");
    private static final EnrichConfig HEIGHTS_ENRICH = new EnrichConfig("heights_policy", "enrich-policy-heights.json");
    private static final EnrichConfig DECADES_ENRICH = new EnrichConfig("decades_policy", "enrich-policy-decades.json");
    private static final EnrichConfig CITY_NAMES_ENRICH = new EnrichConfig("city_names", "enrich-policy-city_names.json");
    private static final EnrichConfig CITY_BOUNDARIES_ENRICH = new EnrichConfig("city_boundaries", "enrich-policy-city_boundaries.json");
    private static final EnrichConfig CITY_AIRPORTS_ENRICH = new EnrichConfig("city_airports", "enrich-policy-city_airports.json");

    public static final List<String> ENRICH_SOURCE_INDICES = List.of(
        "languages",
        "clientips",
        "client_cidr",
        "ages",
        "heights",
        "decades",
        "airport_city_boundaries"
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
        CITY_AIRPORTS_ENRICH
    );

    /**
     * <p>
     * Loads spec data on a local ES server.
     * </p>
     * <p>
     * Accepts an URL as first argument, eg. http://localhost:9200 or http://user:pass@localhost:9200
     * </p>
     * <p>
     * If no arguments are specified, the default URL is http://localhost:9200 without authentication
     * </p>
     * <p>
     * It also supports HTTPS
     * </p>
     *
     * @param args the URL to connect
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // Need to setup the log configuration properly to avoid messages when creating a new RestClient
        PluginManager.addPackage(LogConfigurator.class.getPackage().getName());
        LogConfigurator.configureESLogging();

        String protocol = "http";
        String host = "localhost";
        int port = 9200;
        String username = null;
        String password = null;
        if (args.length > 0) {
            URL url = URI.create(args[0]).toURL();
            protocol = url.getProtocol();
            host = url.getHost();
            port = url.getPort();
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("Please specify a valid port [0 - 65535], found [" + port + "]");
            }
            String userInfo = url.getUserInfo();
            if (userInfo != null) {
                if (userInfo.contains(":") == false || userInfo.split(":").length != 2) {
                    throw new IllegalArgumentException("Invalid user credentials [username:password], found [" + userInfo + "]");
                }
                String[] userPw = userInfo.split(":");
                username = userPw[0];
                password = userPw[1];
            }
        }
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, protocol));
        if (username != null) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder = builder.setHttpClientConfigCallback(
                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            );
        }

        try (RestClient client = builder.build()) {
            loadDataSetIntoEs(client, true, (restClient, indexName, indexMapping, indexSettings) -> {
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
    }

    public static Set<TestDataset> availableDatasetsForEs(RestClient client, boolean supportsIndexModeLookup) throws IOException {
        boolean inferenceEnabled = clusterHasInferenceEndpoint(client);

        Set<TestDataset> testDataSets = new HashSet<>();

        for (TestDataset dataset : CSV_DATASET_MAP.values()) {
            if ((inferenceEnabled || dataset.requiresInferenceEndpoint == false)
                && (supportsIndexModeLookup || isLookupDataset(dataset) == false)) {
                testDataSets.add(dataset);
            }
        }

        return testDataSets;
    }

    public static boolean isLookupDataset(TestDataset dataset) throws IOException {
        Settings settings = dataset.readSettingsFile();
        String mode = settings.get("index.mode");
        return (mode != null && mode.equalsIgnoreCase("lookup"));
    }

    public static void loadDataSetIntoEs(RestClient client, boolean supportsIndexModeLookup) throws IOException {
        loadDataSetIntoEs(client, supportsIndexModeLookup, (restClient, indexName, indexMapping, indexSettings) -> {
            ESRestTestCase.createIndex(restClient, indexName, indexSettings, indexMapping, null);
        });
    }

    private static void loadDataSetIntoEs(RestClient client, boolean supportsIndexModeLookup, IndexCreator indexCreator)
        throws IOException {
        Logger logger = LogManager.getLogger(CsvTestsDataLoader.class);

        Set<String> loadedDatasets = new HashSet<>();
        for (var dataset : availableDatasetsForEs(client, supportsIndexModeLookup)) {
            load(client, dataset, logger, indexCreator);
            loadedDatasets.add(dataset.indexName);
        }
        forceMerge(client, loadedDatasets, logger);
        for (var policy : ENRICH_POLICIES) {
            loadEnrichPolicy(client, policy.policyName, policy.policyFileName, logger);
        }
    }

    /**
     * The semantic_text mapping type require an inference endpoint that needs to be setup before creating the index.
     */
    public static void createInferenceEndpoint(RestClient client) throws IOException {
        Request request = new Request("PUT", "_inference/sparse_embedding/test_sparse_inference");
        request.setJsonEntity("""
                  {
                   "service": "test_service",
                   "service_settings": {
                     "model": "my_model",
                     "api_key": "abc64"
                   },
                   "task_settings": {
                   }
                 }
            """);
        client.performRequest(request);
    }

    public static void deleteInferenceEndpoint(RestClient client) throws IOException {
        try {
            client.performRequest(new Request("DELETE", "_inference/sparse_embedding/test_sparse_inference"));
        } catch (ResponseException e) {
            // 404 here means the endpoint was not created
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public static boolean clusterHasInferenceEndpoint(RestClient client) throws IOException {
        Request request = new Request("GET", "_inference/sparse_embedding/test_sparse_inference");
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

    private static void loadEnrichPolicy(RestClient client, String policyName, String policyFileName, Logger logger) throws IOException {
        URL policyMapping = CsvTestsDataLoader.class.getResource("/" + policyFileName);
        if (policyMapping == null) {
            throw new IllegalArgumentException("Cannot find resource " + policyFileName);
        }
        String entity = readTextFile(policyMapping);
        Request request = new Request("PUT", "/_enrich/policy/" + policyName);
        request.setJsonEntity(entity);
        client.performRequest(request);

        request = new Request("POST", "/_enrich/policy/" + policyName + "/_execute");
        client.performRequest(request);
    }

    private static void load(RestClient client, TestDataset dataset, Logger logger, IndexCreator indexCreator) throws IOException {
        final String mappingName = "/" + dataset.mappingFileName;
        URL mapping = CsvTestsDataLoader.class.getResource(mappingName);
        if (mapping == null) {
            throw new IllegalArgumentException("Cannot find resource " + mappingName);
        }
        final String dataName = "/data/" + dataset.dataFileName;
        URL data = CsvTestsDataLoader.class.getResource(dataName);
        if (data == null) {
            throw new IllegalArgumentException("Cannot find resource " + dataName);
        }

        Settings indexSettings = dataset.readSettingsFile();
        indexCreator.createIndex(client, dataset.indexName, readMappingFile(mapping, dataset.typeMapping), indexSettings);
        loadCsvData(client, dataset.indexName, data, dataset.allowSubFields, logger);
    }

    private static String readMappingFile(URL resource, Map<String, String> typeMapping) throws IOException {
        String mappingJsonText = readTextFile(resource);
        if (typeMapping == null || typeMapping.isEmpty()) {
            return mappingJsonText;
        }
        boolean modified = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mappingNode = mapper.readTree(mappingJsonText);
        JsonNode propertiesNode = mappingNode.path("properties");

        for (Map.Entry<String, String> entry : typeMapping.entrySet()) {
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

    public static String readTextFile(URL resource) throws IOException {
        try (BufferedReader reader = reader(resource)) {
            StringBuilder b = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                b.append(line);
            }
            return b.toString();
        }
    }

    @SuppressWarnings("unchecked")
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
    private static void loadCsvData(RestClient client, String indexName, URL resource, boolean allowSubFields, Logger logger)
        throws IOException {
        ArrayList<String> failures = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = reader(resource)) {
            String line;
            int lineNumber = 1;
            String[] columns = null; // list of column names. If one column name contains dot, it is a subfield and its value will be null
            List<Integer> subFieldsIndices = new ArrayList<>(); // list containing the index of a subfield in "columns" String[]

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (line.isEmpty() == false && line.startsWith("//") == false) {
                    String[] entries = multiValuesAwareCsvToStringArray(line, lineNumber);
                    // the schema row
                    if (columns == null) {
                        columns = new String[entries.length];
                        for (int i = 0; i < entries.length; i++) {
                            int split = entries[i].indexOf(':');
                            if (split < 0) {
                                columns[i] = entries[i].trim();
                            } else {
                                String name = entries[i].substring(0, split).trim();
                                if (allowSubFields || name.contains(".") == false) {
                                    columns[i] = name;
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
                                if ("_id".equals(columns[i])) {
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
                                            rowStringValue.append(quoteIfNecessary(s)).append(",");
                                        }
                                        // remove the last comma and put a closing bracket instead
                                        rowStringValue.replace(rowStringValue.length() - 1, rowStringValue.length(), "]");
                                        entries[i] = rowStringValue.toString();
                                    } else {
                                        entries[i] = quoteIfNecessary(entries[i]);
                                    }
                                    // replace any escaped commas with single comma
                                    entries[i] = entries[i].replace(ESCAPED_COMMA_SEQUENCE, ",");
                                    row.append("\"").append(columns[i]).append("\":").append(entries[i]);
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
                    sendBulkRequest(indexName, builder, client, logger, failures);
                    builder.setLength(0);
                }
            }
        }
        if (builder.isEmpty() == false) {
            sendBulkRequest(indexName, builder, client, logger, failures);
        }
        if (failures.isEmpty() == false) {
            for (String failure : failures) {
                logger.error(failure);
            }
            throw new IOException("Data loading failed with " + failures.size() + " errors: " + failures.get(0));
        }
    }

    private static String quoteIfNecessary(String value) {
        boolean isQuoted = (value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("{") && value.endsWith("}"));
        return isQuoted ? value : "\"" + value + "\"";
    }

    private static void sendBulkRequest(String indexName, StringBuilder builder, RestClient client, Logger logger, List<String> failures)
        throws IOException {
        // The indexName is optional for a bulk request, but we use it for routing in MultiClusterSpecIT.
        builder.append("\n");
        logger.debug("Sending bulk request of [{}] bytes for [{}]", builder.length(), indexName);
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
                if (Boolean.FALSE.equals(errors)) {
                    logger.info("Data loading of [{}] bytes into [{}] OK", builder.length(), indexName);
                } else {
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

    private static void forceMerge(RestClient client, Set<String> indices, Logger logger) throws IOException {
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

    public record TestDataset(
        String indexName,
        String mappingFileName,
        String dataFileName,
        String settingFileName,
        boolean allowSubFields,
        @Nullable Map<String, String> typeMapping,
        boolean requiresInferenceEndpoint
    ) {
        public TestDataset(String indexName, String mappingFileName, String dataFileName) {
            this(indexName, mappingFileName, dataFileName, null, true, null, false);
        }

        public TestDataset(String indexName) {
            this(indexName, "mapping-" + indexName + ".json", indexName + ".csv", null, true, null, false);
        }

        public TestDataset withIndex(String indexName) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                requiresInferenceEndpoint
            );
        }

        public TestDataset withData(String dataFileName) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                requiresInferenceEndpoint
            );
        }

        public TestDataset withSetting(String settingFileName) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                requiresInferenceEndpoint
            );
        }

        public TestDataset noSubfields() {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                false,
                typeMapping,
                requiresInferenceEndpoint
            );
        }

        public TestDataset withTypeMapping(Map<String, String> typeMapping) {
            return new TestDataset(
                indexName,
                mappingFileName,
                dataFileName,
                settingFileName,
                allowSubFields,
                typeMapping,
                requiresInferenceEndpoint
            );
        }

        public TestDataset withInferenceEndpoint(boolean needsInference) {
            return new TestDataset(indexName, mappingFileName, dataFileName, settingFileName, allowSubFields, typeMapping, needsInference);
        }

        private Settings readSettingsFile() throws IOException {
            Settings indexSettings = Settings.EMPTY;
            final String settingName = settingFileName != null ? "/" + settingFileName : null;
            if (settingName != null) {
                indexSettings = Settings.builder()
                    .loadFromStream(settingName, CsvTestsDataLoader.class.getResourceAsStream(settingName), false)
                    .build();
            }

            return indexSettings;
        }
    }

    public record EnrichConfig(String policyName, String policyFileName) {}

    private interface IndexCreator {
        void createIndex(RestClient client, String indexName, String mapping, Settings indexSettings) throws IOException;
    }
}
