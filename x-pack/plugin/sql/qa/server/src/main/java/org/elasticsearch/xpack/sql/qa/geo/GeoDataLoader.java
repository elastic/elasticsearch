/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.geo;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.qa.jdbc.SqlSpecTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.sql.qa.jdbc.DataLoader.createString;
import static org.elasticsearch.xpack.sql.qa.jdbc.DataLoader.readFromJarUrl;

public class GeoDataLoader {

    public static void main(String[] args) throws Exception {
        try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            loadOGCDatasetIntoEs(client, "ogc");
            loadGeoDatasetIntoEs(client, "geo");
            Loggers.getLogger(GeoDataLoader.class).info("Geo data loaded");
        }
    }

    protected static void loadOGCDatasetIntoEs(RestClient client, String index) throws Exception {
        createIndex(client, index, createOGCIndexRequest());
        loadData(client, index, readResource("/ogc/ogc.json"));
        List<String> aliases = """
            lakes
            road_segments
            divided_routes
            forests
            bridges
            streams
            buildings
            ponds
            named_places
            map_neatlines
            """.lines().toList();
        for (String alias : aliases) {
            makeFilteredAlias(client, alias, index, String.format(java.util.Locale.ROOT, "\"term\" : { \"ogc_type\" : \"%s\" }", alias));
        }
    }

    private static String createOGCIndexRequest() throws Exception {
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
                // Common
                createIndex.startObject("ogc_type").field("type", "keyword").endObject();
                createIndex.startObject("fid").field("type", "integer").endObject();
                createString("name", createIndex);

                // Type specific
                createIndex.startObject("shore").field("type", "shape").endObject(); // lakes

                createString("aliases", createIndex); // road_segments
                createIndex.startObject("num_lanes").field("type", "integer").endObject(); // road_segments, divided_routes
                createIndex.startObject("centerline").field("type", "shape").endObject(); // road_segments, streams

                createIndex.startObject("centerlines").field("type", "shape").endObject(); // divided_routes

                createIndex.startObject("boundary").field("type", "shape").endObject(); // forests, named_places

                createIndex.startObject("position").field("type", "shape").endObject(); // bridges, buildings

                createString("address", createIndex); // buildings
                createIndex.startObject("footprint").field("type", "shape").endObject(); // buildings

                createIndex.startObject("type").field("type", "keyword").endObject(); // ponds
                createIndex.startObject("shores").field("type", "shape").endObject(); // ponds

                createIndex.startObject("neatline").field("type", "shape").endObject(); // map_neatlines

            }
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        return Strings.toString(createIndex);
    }

    private static void createIndex(RestClient client, String index, String settingsMappings) throws IOException {
        Request createIndexRequest = new Request("PUT", "/" + index);
        createIndexRequest.setEntity(new StringEntity(settingsMappings, ContentType.APPLICATION_JSON));
        client.performRequest(createIndexRequest);
    }

    static void loadGeoDatasetIntoEs(RestClient client, String index) throws Exception {
        createIndex(client, index, readResource("/geo/geosql.json"));
        loadData(client, index, readResource("/geo/geosql-bulk.json"));
    }

    private static void loadData(RestClient client, String index, String bulk) throws IOException {
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk);
        Response response = client.performRequest(request);

        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new RuntimeException("Cannot load data " + response.getStatusLine());
        }

        String bulkResponseStr = EntityUtils.toString(response.getEntity());
        Map<String, Object> bulkResponseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, bulkResponseStr, false);

        if ((boolean) bulkResponseMap.get("errors")) {
            throw new RuntimeException("Failed to load bulk data " + bulkResponseStr);
        }
    }

    public static void makeFilteredAlias(RestClient client, String aliasName, String index, String filter) throws Exception {
        Request request = new Request("POST", "/" + index + "/_alias/" + aliasName);
        request.setJsonEntity("{\"filter\" : { " + filter + " } }");
        client.performRequest(request);
    }

    private static String readResource(String location) throws IOException {
        URL dataSet = SqlSpecTestCase.class.getResource(location);
        if (dataSet == null) {
            throw new IllegalArgumentException("Can't find [" + location + "]");
        }
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(dataSet), StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            while (line != null) {
                if (line.trim().startsWith("//") == false) {
                    builder.append(line);
                    builder.append('\n');
                }
                line = reader.readLine();
            }
            return builder.toString();
        }
    }

}
