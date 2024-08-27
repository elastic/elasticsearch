/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
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
    private static final TestsDataset EMPLOYEES = new TestsDataset("employees", "mapping-default.json", "employees.csv", null, false);
    private static final TestsDataset HOSTS = new TestsDataset("hosts", "mapping-hosts.json", "hosts.csv");
    private static final TestsDataset APPS = new TestsDataset("apps", "mapping-apps.json", "apps.csv");
    private static final TestsDataset LANGUAGES = new TestsDataset("languages", "mapping-languages.json", "languages.csv");
    private static final TestsDataset ALERTS = new TestsDataset("alerts", "mapping-alerts.json", "alerts.csv");
    private static final TestsDataset UL_LOGS = new TestsDataset("ul_logs", "mapping-ul_logs.json", "ul_logs.csv");
    private static final TestsDataset SAMPLE_DATA = new TestsDataset("sample_data", "mapping-sample_data.json", "sample_data.csv");
    private static final TestsDataset SAMPLE_DATA_STR = new TestsDataset(
        "sample_data_str",
        "mapping-sample_data_str.json",
        "sample_data_str.csv"
    );
    private static final TestsDataset SAMPLE_DATA_TS_LONG = new TestsDataset(
        "sample_data_ts_long",
        "mapping-sample_data_ts_long.json",
        "sample_data_ts_long.csv"
    );
    private static final TestsDataset CLIENT_IPS = new TestsDataset("clientips", "mapping-clientips.json", "clientips.csv");
    private static final TestsDataset CLIENT_CIDR = new TestsDataset("client_cidr", "mapping-client_cidr.json", "client_cidr.csv");
    private static final TestsDataset AGES = new TestsDataset("ages", "mapping-ages.json", "ages.csv");
    private static final TestsDataset HEIGHTS = new TestsDataset("heights", "mapping-heights.json", "heights.csv");
    private static final TestsDataset DECADES = new TestsDataset("decades", "mapping-decades.json", "decades.csv");
    private static final TestsDataset AIRPORTS = new TestsDataset("airports", "mapping-airports.json", "airports.csv");
    private static final TestsDataset AIRPORTS_MP = new TestsDataset("airports_mp", "mapping-airports.json", "airports_mp.csv");
    private static final TestsDataset AIRPORTS_WEB = new TestsDataset("airports_web", "mapping-airports_web.json", "airports_web.csv");
    private static final TestsDataset DATE_NANOS = new TestsDataset("date_nanos", "mapping-date_nanos.json", "date_nanos.csv");
    private static final TestsDataset COUNTRIES_BBOX = new TestsDataset(
        "countries_bbox",
        "mapping-countries_bbox.json",
        "countries_bbox.csv"
    );
    private static final TestsDataset COUNTRIES_BBOX_WEB = new TestsDataset(
        "countries_bbox_web",
        "mapping-countries_bbox_web.json",
        "countries_bbox_web.csv"
    );
    private static final TestsDataset AIRPORT_CITY_BOUNDARIES = new TestsDataset(
        "airport_city_boundaries",
        "mapping-airport_city_boundaries.json",
        "airport_city_boundaries.csv"
    );
    private static final TestsDataset CARTESIAN_MULTIPOLYGONS = new TestsDataset(
        "cartesian_multipolygons",
        "mapping-cartesian_multipolygons.json",
        "cartesian_multipolygons.csv"
    );
    private static final TestsDataset DISTANCES = new TestsDataset("distances", "mapping-distances.json", "distances.csv");
    private static final TestsDataset K8S = new TestsDataset("k8s", "k8s-mappings.json", "k8s.csv", "k8s-settings.json", true);
    private static final TestsDataset ADDRESSES = new TestsDataset("addresses", "mapping-addresses.json", "addresses.csv", null, true);
    private static final TestsDataset BOOKS = new TestsDataset("books", "mapping-books.json", "books.csv", null, true);

    public static final Map<String, TestsDataset> CSV_DATASET_MAP = Map.ofEntries(
        Map.entry(EMPLOYEES.indexName, EMPLOYEES),
        Map.entry(HOSTS.indexName, HOSTS),
        Map.entry(APPS.indexName, APPS),
        Map.entry(LANGUAGES.indexName, LANGUAGES),
        Map.entry(UL_LOGS.indexName, UL_LOGS),
        Map.entry(SAMPLE_DATA.indexName, SAMPLE_DATA),
        Map.entry(ALERTS.indexName, ALERTS),
        Map.entry(SAMPLE_DATA_STR.indexName, SAMPLE_DATA_STR),
        Map.entry(SAMPLE_DATA_TS_LONG.indexName, SAMPLE_DATA_TS_LONG),
        Map.entry(CLIENT_IPS.indexName, CLIENT_IPS),
        Map.entry(CLIENT_CIDR.indexName, CLIENT_CIDR),
        Map.entry(AGES.indexName, AGES),
        Map.entry(HEIGHTS.indexName, HEIGHTS),
        Map.entry(DECADES.indexName, DECADES),
        Map.entry(AIRPORTS.indexName, AIRPORTS),
        Map.entry(AIRPORTS_MP.indexName, AIRPORTS_MP),
        Map.entry(AIRPORTS_WEB.indexName, AIRPORTS_WEB),
        Map.entry(COUNTRIES_BBOX.indexName, COUNTRIES_BBOX),
        Map.entry(COUNTRIES_BBOX_WEB.indexName, COUNTRIES_BBOX_WEB),
        Map.entry(AIRPORT_CITY_BOUNDARIES.indexName, AIRPORT_CITY_BOUNDARIES),
        Map.entry(CARTESIAN_MULTIPOLYGONS.indexName, CARTESIAN_MULTIPOLYGONS),
        Map.entry(DATE_NANOS.indexName, DATE_NANOS),
        Map.entry(K8S.indexName, K8S),
        Map.entry(DISTANCES.indexName, DISTANCES),
        Map.entry(ADDRESSES.indexName, ADDRESSES),
        Map.entry(BOOKS.indexName, BOOKS)
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
            loadDataSetIntoEs(client, (restClient, indexName, indexMapping, indexSettings) -> {
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

    private static void loadDataSetIntoEs(RestClient client, IndexCreator indexCreator) throws IOException {
        loadDataSetIntoEs(client, LogManager.getLogger(CsvTestsDataLoader.class), indexCreator);
    }

    public static void loadDataSetIntoEs(RestClient client) throws IOException {
        loadDataSetIntoEs(client, (restClient, indexName, indexMapping, indexSettings) -> {
            ESRestTestCase.createIndex(restClient, indexName, indexSettings, indexMapping, null);
        });
    }

    public static void loadDataSetIntoEs(RestClient client, Logger logger) throws IOException {
        loadDataSetIntoEs(client, logger, (restClient, indexName, indexMapping, indexSettings) -> {
            ESRestTestCase.createIndex(restClient, indexName, indexSettings, indexMapping, null);
        });
    }

    private static void loadDataSetIntoEs(RestClient client, Logger logger, IndexCreator indexCreator) throws IOException {
        for (var dataSet : CSV_DATASET_MAP.values()) {
            final String settingName = dataSet.settingFileName != null ? "/" + dataSet.settingFileName : null;
            load(
                client,
                dataSet.indexName,
                "/" + dataSet.mappingFileName,
                settingName,
                "/" + dataSet.dataFileName,
                dataSet.allowSubFields,
                logger,
                indexCreator
            );
        }
        forceMerge(client, CSV_DATASET_MAP.keySet(), logger);
        for (var policy : ENRICH_POLICIES) {
            loadEnrichPolicy(client, policy.policyName, policy.policyFileName, logger);
        }
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

    private static void load(
        RestClient client,
        String indexName,
        String mappingName,
        String settingName,
        String dataName,
        boolean allowSubFields,
        Logger logger,
        IndexCreator indexCreator
    ) throws IOException {
        URL mapping = CsvTestsDataLoader.class.getResource(mappingName);
        if (mapping == null) {
            throw new IllegalArgumentException("Cannot find resource " + mappingName);
        }
        URL data = CsvTestsDataLoader.class.getResource(dataName);
        if (data == null) {
            throw new IllegalArgumentException("Cannot find resource " + dataName);
        }
        Settings indexSettings = Settings.EMPTY;
        if (settingName != null) {
            indexSettings = Settings.builder()
                .loadFromStream(settingName, CsvTestsDataLoader.class.getResourceAsStream(settingName), false)
                .build();
        }
        indexCreator.createIndex(client, indexName, readTextFile(mapping), indexSettings);
        loadCsvData(client, indexName, data, allowSubFields, CsvTestsDataLoader::createParser, logger);
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
    private static void loadCsvData(
        RestClient client,
        String indexName,
        URL resource,
        boolean allowSubFields,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p,
        Logger logger
    ) throws IOException {
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
                            String name, typeName;

                            if (split < 0) {
                                throw new IllegalArgumentException(
                                    "A type is always expected in the schema definition; found " + entries[i]
                                );
                            } else {
                                name = entries[i].substring(0, split).trim();
                                if (allowSubFields || name.contains(".") == false) {
                                    typeName = entries[i].substring(split + 1).trim();
                                    if (typeName.isEmpty()) {
                                        throw new IllegalArgumentException(
                                            "A type is always expected in the schema definition; found " + entries[i]
                                        );
                                    }
                                } else {// if it's a subfield, ignore it in the _bulk request
                                    name = null;
                                    subFieldsIndices.add(i);
                                }
                            }
                            columns[i] = name;
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

    private static XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        NamedXContentRegistry contentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(contentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        return xContent.createParser(config, data);
    }

    public record TestsDataset(
        String indexName,
        String mappingFileName,
        String dataFileName,
        String settingFileName,
        boolean allowSubFields
    ) {
        public TestsDataset(String indexName, String mappingFileName, String dataFileName) {
            this(indexName, mappingFileName, dataFileName, null, true);
        }
    }

    public record EnrichConfig(String policyName, String policyFileName) {}

    private interface IndexCreator {
        void createIndex(RestClient client, String indexName, String mapping, Settings indexSettings) throws IOException;
    }
}
