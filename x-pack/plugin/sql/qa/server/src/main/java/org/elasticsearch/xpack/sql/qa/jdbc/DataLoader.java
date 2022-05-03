/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataLoader {

    public static void main(String[] args) throws Exception {
        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            loadEmpDatasetIntoEs(client);
            loadDocsDatasetIntoEs(client);
            LogManager.getLogger(DataLoader.class).info("Data loaded");
        }
    }

    protected static void loadDatasetIntoEs(RestClient client) throws Exception {
        loadEmpDatasetIntoEs(client);
    }

    public static void createEmptyIndex(RestClient client, String index) throws Exception {
        Request request = new Request("PUT", "/" + index);
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("settings");
        {
            createIndex.field("number_of_shards", 1);
            createIndex.field("number_of_replicas", 1);
        }
        createIndex.endObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {}
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        request.setJsonEntity(Strings.toString(createIndex));
        client.performRequest(request);
    }

    protected static void loadEmpDatasetIntoEs(RestClient client) throws Exception {
        loadEmpDatasetIntoEs(client, "test_emp", "employees");
        loadEmpDatasetWithExtraIntoEs(client, "test_emp_copy", "employees");
        loadLogsDatasetIntoEs(client, "logs", "logs");
        loadLogNanosDatasetIntoEs(client, "logs_nanos", "logs_nanos");
        loadLogUnsignedLongIntoEs(client, "logs_unsigned_long", "logs_unsigned_long");
        makeAlias(client, "test_alias", "test_emp", "test_emp_copy");
        makeAlias(client, "test_alias_emp", "test_emp", "test_emp_copy");
        // frozen index
        loadEmpDatasetIntoEs(client, "frozen_emp", "employees");
        freeze(client, "frozen_emp");
        loadNoColsDatasetIntoEs(client, "empty_mapping");
    }

    private static void loadNoColsDatasetIntoEs(RestClient client, String index) throws Exception {
        createEmptyIndex(client, index);
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            {"index":{}}
            {}
            {"index":{}}
            {}
            """);
        client.performRequest(request);
    }

    public static void loadDocsDatasetIntoEs(RestClient client) throws Exception {
        loadEmpDatasetIntoEs(client, "emp", "employees");
        loadLibDatasetIntoEs(client, "library");
        makeAlias(client, "employees", "emp");
        // frozen index
        loadLibDatasetIntoEs(client, "archive");
        freeze(client, "archive");
    }

    public static void createString(String name, XContentBuilder builder) throws Exception {
        builder.startObject(name)
            .field("type", "text")
            .startObject("fields")
            .startObject("keyword")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
    }

    protected static void loadEmpDatasetIntoEs(RestClient client, String index, String fileName) throws Exception {
        loadEmpDatasetIntoEs(client, index, fileName, false);
    }

    protected static void loadEmpDatasetWithExtraIntoEs(RestClient client, String index, String fileName) throws Exception {
        loadEmpDatasetIntoEs(client, index, fileName, true);
    }

    private static void loadEmpDatasetIntoEs(RestClient client, String index, String fileName, boolean extraFields) throws Exception {
        Request request = new Request("PUT", "/" + index);
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("settings");
        {
            createIndex.field("number_of_shards", 1);
        }
        createIndex.endObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createIndex.startObject("emp_no").field("type", "integer");
                if (extraFields) {
                    createIndex.field("copy_to", "extra_no");
                }
                createIndex.endObject();
                if (extraFields) {
                    createIndex.startObject("extra_no").field("type", "integer").endObject();
                }
                createString("first_name", createIndex);
                createString("last_name", createIndex);
                createIndex.startObject("gender").field("type", "keyword");
                createIndex.endObject();

                if (extraFields) {
                    createIndex.startObject("extra_gender").field("type", "constant_keyword").endObject();
                    createIndex.startObject("null_constant").field("type", "constant_keyword").endObject();
                    createIndex.startObject("wildcard_name").field("type", "wildcard").endObject();
                    createIndex.startObject("extra.info.gender").field("type", "alias").field("path", "gender").endObject();
                }

                createIndex.startObject("birth_date").field("type", "date").endObject();
                createIndex.startObject("hire_date").field("type", "date").endObject();
                createIndex.startObject("salary").field("type", "integer").endObject();
                if (extraFields) {
                    createIndex.startObject("salary_ul").field("type", "unsigned_long").endObject();
                }
                createIndex.startObject("languages").field("type", "byte").endObject();
                {
                    createIndex.startObject("dep").field("type", "nested");
                    createIndex.startObject("properties");
                    createIndex.startObject("dep_id").field("type", "keyword").endObject();
                    createString("dep_name", createIndex);
                    createIndex.startObject("from_date").field("type", "date").endObject();
                    createIndex.startObject("to_date").field("type", "date").endObject();
                    createIndex.endObject();
                    createIndex.endObject();
                }
            }
            createIndex.endObject();
            // define the runtime field
            createIndex.startObject("runtime");
            {
                createIndex.startObject("name").field("type", "keyword");
                createIndex.startObject("script")
                    .field(
                        "source",
                        "if (doc['first_name.keyword'].size()==0) emit(' '.concat(doc['last_name.keyword'].value));"
                            + " else emit(doc['first_name.keyword'].value.concat(' ').concat(doc['last_name.keyword'].value))"
                    );
                createIndex.endObject();
                createIndex.endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        request.setJsonEntity(Strings.toString(createIndex));
        client.performRequest(request);

        Map<String, String> deps = new LinkedHashMap<>();
        csvToLines("departments", (titles, fields) -> deps.put(fields.get(0), fields.get(1)));

        Map<String, List<List<String>>> dep_emp = new LinkedHashMap<>();
        csvToLines("dep_emp", (titles, fields) -> {
            String emp_no = fields.get(0);
            List<List<String>> list = dep_emp.get(emp_no);
            if (list == null) {
                list = new ArrayList<>();
                dep_emp.put(emp_no, list);
            }
            List<String> dep = new ArrayList<>();
            // dep_id
            dep.add(fields.get(1));
            // dep_name (from departments)
            dep.add(deps.get(fields.get(1)));
            // from
            dep.add(fields.get(2));
            // to
            dep.add(fields.get(3));
            list.add(dep);
        });

        request = new Request("POST", "/" + index + "/_bulk?refresh=wait_for");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        csvToLines(fileName, (titles, fields) -> {
            bulk.append("{\"index\":{}}\n");
            bulk.append('{');
            String emp_no = fields.get(1);

            boolean hadLastItem = false;

            String wildcard_name = null;
            boolean setWildcardName = true;
            for (int f = 0; f < fields.size(); f++) {
                // an empty value in the csv file is treated as 'null', thus skipping it in the bulk request
                if (fields.get(f).trim().length() > 0) {
                    if (hadLastItem) {
                        bulk.append(",");
                    }
                    hadLastItem = true;
                    bulk.append('"').append(titles.get(f)).append("\":\"").append(fields.get(f)).append('"');
                    if (extraFields) {
                        if (titles.get(f).equals("gender")) {
                            bulk.append(",\"extra_gender\":\"Female\"");
                        } else if (titles.get(f).equals("salary")) {
                            bulk.append(",\"salary_ul\":" + fields.get(f));
                        }
                    }
                }
                if ((titles.get(f).equals("first_name") || titles.get(f).equals("last_name")) && extraFields && setWildcardName) {
                    if (fields.get(f).trim().length() == 0) {
                        setWildcardName = false;
                    } else {
                        wildcard_name = wildcard_name == null ? fields.get(f) : wildcard_name + " " + fields.get(f);
                    }
                }
            }
            // append the wildcard field
            if (extraFields && setWildcardName) {
                bulk.append(",\"wildcard_name\":\"" + wildcard_name + "\"");
            }

            // append department
            List<List<String>> list = dep_emp.get(emp_no);
            if (list.isEmpty() == false) {
                bulk.append(", \"dep\" : [");
                for (List<String> dp : list) {
                    bulk.append("{");
                    bulk.append("\"dep_id\":\"" + dp.get(0) + "\",");
                    bulk.append("\"dep_name\":\"" + dp.get(1) + "\",");
                    bulk.append("\"from_date\":\"" + dp.get(2) + "\",");
                    bulk.append("\"to_date\":\"" + dp.get(3) + "\"");
                    bulk.append("},");
                }
                // remove last ,
                bulk.setLength(bulk.length() - 1);
                bulk.append("]");
            }

            bulk.append("}\n");
        });
        request.setJsonEntity(bulk.toString());
        client.performRequest(request);
    }

    protected static void loadLogsDatasetIntoEs(RestClient client, String index, String filename) throws Exception {
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createIndex.startObject("id").field("type", "integer").endObject();
                createIndex.startObject("@timestamp").field("type", "date").endObject();
                createIndex.startObject("bytes_in").field("type", "integer").endObject();
                createIndex.startObject("bytes_out").field("type", "integer").endObject();
                createIndex.startObject("client_ip").field("type", "ip").endObject();
                createIndex.startObject("client_port").field("type", "integer").endObject();
                createIndex.startObject("dest_ip").field("type", "ip").endObject();
                createIndex.startObject("status").field("type", "keyword").endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject();

        loadDatasetIntoEs(client, index, filename, createIndex);
    }

    protected static void loadLogNanosDatasetIntoEs(RestClient client, String index, String filename) throws Exception {
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createIndex.startObject("id").field("type", "integer").endObject();
                createIndex.startObject("@timestamp").field("type", "date_nanos").endObject();
                createIndex.startObject("status").field("type", "keyword").endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject();

        loadDatasetIntoEs(client, index, filename, createIndex);
    }

    protected static void loadLogUnsignedLongIntoEs(RestClient client, String index, String filename) throws Exception {
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createIndex.startObject("id").field("type", "integer").endObject();
                createIndex.startObject("@timestamp").field("type", "date_nanos").endObject();
                createIndex.startObject("bytes_in").field("type", "unsigned_long").endObject();
                createIndex.startObject("bytes_out").field("type", "unsigned_long").endObject();
                createIndex.startObject("status").field("type", "keyword").endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject();

        loadDatasetIntoEs(client, index, filename, createIndex);
    }

    protected static void loadLibDatasetIntoEs(RestClient client, String index) throws Exception {
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createString("name", createIndex);
                createString("author", createIndex);
                createIndex.startObject("release_date").field("type", "date").endObject();
                createIndex.startObject("page_count").field("type", "short").endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject();

        loadDatasetIntoEs(client, index, "library", createIndex);
    }

    // createIndex must be startObject()'d, but not endObject()'d: the index settings are added at the end
    protected static void loadDatasetIntoEs(RestClient client, String index, String filename, XContentBuilder createIndex)
        throws Exception {
        createIndex.startObject("settings");
        {
            createIndex.field("number_of_shards", 1);
            createIndex.field("number_of_replicas", 1);
        }
        createIndex.endObject();
        createIndex.endObject();

        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(Strings.toString(createIndex));
        client.performRequest(request);

        request = new Request("POST", "/" + index + "/_bulk?refresh=wait_for");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        csvToLines(filename, (titles, fields) -> {
            bulk.append("{\"index\":{\"_id\":\"" + fields.get(0) + "\"}}\n");
            bulk.append("{");
            for (int f = 0; f < titles.size(); f++) {
                if (fields.size() > f && Strings.hasText(fields.get(f))) {
                    if (f > 0) {
                        bulk.append(",");
                    }
                    bulk.append('"').append(titles.get(f)).append("\":\"").append(fields.get(f)).append('"');
                }
            }
            bulk.append("}\n");
        });
        request.setJsonEntity(bulk.toString());
        client.performRequest(request);
    }

    public static void makeAlias(RestClient client, String aliasName, String... indices) throws Exception {
        for (String index : indices) {
            client.performRequest(new Request("POST", "/" + index + "/_alias/" + aliasName));
        }
    }

    protected static void freeze(RestClient client, String... indices) throws Exception {
        for (String index : indices) {
            Request freezeRequest = new Request("POST", "/" + index + "/_freeze");
            client.performRequest(freezeRequest);
        }
    }

    private static void csvToLines(String name, CheckedBiConsumer<List<String>, List<String>, Exception> consumeLine) throws Exception {
        String location = "/" + name + ".csv";
        URL dataSet = SqlSpecTestCase.class.getResource(location);
        if (dataSet == null) {
            throw new IllegalArgumentException("Can't find [" + location + "]");
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(dataSet), StandardCharsets.UTF_8))) {
            String titlesString = reader.readLine();
            if (titlesString == null) {
                throw new IllegalArgumentException("[" + location + "] must contain at least a title row");
            }
            List<String> titles = Arrays.asList(titlesString.split(","));

            String line;
            while ((line = reader.readLine()) != null) {
                consumeLine.accept(titles, Arrays.asList(line.split(",")));
            }
        }
    }

    @SuppressForbidden(reason = "test reads from jar")
    public static InputStream readFromJarUrl(URL source) throws IOException {
        return source.openStream();
    }
}
