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
import org.elasticsearch.common.logging.LogConfigurator;
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
import org.elasticsearch.xpack.ql.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.Strings.delimitedListToStringArray;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.CsvTestUtils.multiValuesAwareCsvToStringArray;

public class CsvTestsDataLoader {
    private static final TestsDataset EMPLOYEES = new TestsDataset("employees", "mapping-default.json", "employees.csv");
    private static final TestsDataset HOSTS = new TestsDataset("hosts", "mapping-hosts.json", "hosts.csv");
    private static final TestsDataset APPS = new TestsDataset("apps", "mapping-apps.json", "apps.csv");
    private static final TestsDataset LANGUAGES = new TestsDataset("languages", "mapping-languages.json", "languages.csv");

    public static final Map<String, TestsDataset> CSV_DATASET_MAP = Map.of(
        EMPLOYEES.indexName,
        EMPLOYEES,
        HOSTS.indexName,
        HOSTS,
        APPS.indexName,
        APPS,
        LANGUAGES.indexName,
        LANGUAGES
    );

    private static final EnrichConfig LANGUAGES_ENRICH = new EnrichConfig("languages_policy", "enricy-policy-languages.json");

    public static final List<EnrichConfig> ENRICH_POLICIES = List.of(LANGUAGES_ENRICH);

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
            loadDataSetIntoEs(client);
        }
    }

    public static void loadDataSetIntoEs(RestClient client) throws IOException {
        loadDataSetIntoEs(client, LogManager.getLogger(CsvTestsDataLoader.class));
    }

    public static void loadDataSetIntoEs(RestClient client, Logger logger) throws IOException {
        for (var dataSet : CSV_DATASET_MAP.values()) {
            load(client, dataSet.indexName, "/" + dataSet.mappingFileName, "/" + dataSet.dataFileName, logger);
        }
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

    private static void load(RestClient client, String indexName, String mappingName, String dataName, Logger logger) throws IOException {
        URL mapping = CsvTestsDataLoader.class.getResource(mappingName);
        if (mapping == null) {
            throw new IllegalArgumentException("Cannot find resource " + mappingName);
        }
        URL data = CsvTestsDataLoader.class.getResource(dataName);
        if (data == null) {
            throw new IllegalArgumentException("Cannot find resource " + dataName);
        }
        createTestIndex(client, indexName, readTextFile(mapping));
        loadCsvData(client, indexName, data, CsvTestsDataLoader::createParser, logger);
    }

    private static void createTestIndex(RestClient client, String indexName, String mapping) throws IOException {
        ESRestTestCase.createIndex(client, indexName, null, mapping, null);
    }

    public static String readTextFile(URL resource) throws IOException {
        try (BufferedReader reader = TestUtils.reader(resource)) {
            StringBuilder b = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                b.append(line);
            }
            return b.toString();
        }
    }

    @SuppressWarnings("unchecked")
    private static void loadCsvData(
        RestClient client,
        String indexName,
        URL resource,
        CheckedBiFunction<XContent, InputStream, XContentParser, IOException> p,
        Logger logger
    ) throws IOException {
        Request request = new Request("POST", "/_bulk");
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = org.elasticsearch.xpack.ql.TestUtils.reader(resource)) {
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
                            int split = entries[i].indexOf(":");
                            String name, typeName;

                            if (split < 0) {
                                throw new IllegalArgumentException(
                                    "A type is always expected in the schema definition; found " + entries[i]
                                );
                            } else {
                                name = entries[i].substring(0, split).trim();
                                if (name.indexOf(".") < 0) {
                                    typeName = entries[i].substring(split + 1).trim();
                                    if (typeName.length() == 0) {
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
                        for (int i = 0; i < entries.length; i++) {
                            // ignore values that belong to subfields and don't add them to the bulk request
                            if (subFieldsIndices.contains(i) == false) {
                                boolean isValueNull = "".equals(entries[i]);
                                try {
                                    if (isValueNull == false) {
                                        // add a comma after the previous value, only when there was actually a value before
                                        if (i > 0 && row.length() > 0) {
                                            row.append(",");
                                        }
                                        if (entries[i].contains(",")) {// multi-value
                                            StringBuilder rowStringValue = new StringBuilder("[");
                                            for (String s : delimitedListToStringArray(entries[i], ",")) {
                                                rowStringValue.append("\"" + s + "\",");
                                            }
                                            // remove the last comma and put a closing bracket instead
                                            rowStringValue.replace(rowStringValue.length() - 1, rowStringValue.length(), "]");
                                            entries[i] = rowStringValue.toString();
                                        } else {
                                            entries[i] = "\"" + entries[i] + "\"";
                                        }
                                        row.append("\"" + columns[i] + "\":" + entries[i]);
                                    }
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
                        builder.append("{\"index\": {\"_index\":\"" + indexName + "\"}}\n");
                        builder.append("{" + row + "}\n");
                    }
                }
                lineNumber++;
            }
            builder.append("\n");
        }

        request.setJsonEntity(builder.toString());
        request.addParameter("refresh", "wait_for");
        Response response = client.performRequest(request);
        if (response.getStatusLine().getStatusCode() == 200) {
            HttpEntity entity = response.getEntity();
            try (InputStream content = entity.getContent()) {
                XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
                Map<String, Object> result = XContentHelper.convertToMap(xContentType.xContent(), content, false);
                Object errors = result.get("errors");
                if (Boolean.FALSE.equals(errors)) {
                    logger.info("Data loading OK");
                    request = new Request("POST", "/" + indexName + "/_forcemerge?max_num_segments=1");
                    response = client.performRequest(request);
                    if (response.getStatusLine().getStatusCode() != 200) {
                        logger.warn("Force-merge to 1 segment failed: " + response.getStatusLine());
                    } else {
                        logger.info("Forced-merge to 1 segment");
                    }
                } else {
                    logger.error("Data loading FAILED");
                }
            }
        } else {
            logger.error("Error loading data: " + response.getStatusLine());
        }
    }

    private static XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        NamedXContentRegistry contentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(contentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        return xContent.createParser(config, data);
    }

    public record TestsDataset(String indexName, String mappingFileName, String dataFileName) {}

    public record EnrichConfig(String policyName, String policyFileName) {}
}
