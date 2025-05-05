/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class EcsFieldsDiscoverer {

    private static final String ECS_FLAT_FILE_URL = "https://raw.githubusercontent.com/elastic/ecs/main/generated/ecs/ecs_flat.yml";
    private static final String AGENT_FIELDS_PREFIX = "agent.";

    private static final EcsFieldsDiscoverer INSTANCE = new EcsFieldsDiscoverer();

    private final Map<String, String> ecsToOTelAttributeNames = new HashMap<>();
    private final Set<String> ecsResourceFields = new HashSet<>();

    private EcsFieldsDiscoverer() {
        try {
            collectEcsAttributeNames();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to load ECS to OpenTelemetry attribute names", e);
        }
    }

    Map<String, String> getEcsToOTelAttributeNames() {
        return ecsToOTelAttributeNames;
    }

    Set<String> getEcsResourceFields() {
        return ecsResourceFields;
    }

    static EcsFieldsDiscoverer getInstance() {
        return INSTANCE;
    }

    private void collectEcsAttributeNames() throws IOException, InterruptedException {
        Map<String, Object> ecsFields = loadEcsFields();
        for (Map.Entry<String, Object> entry : ecsFields.entrySet()) {
            String ecsName = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> fieldData = (Map<String, Object>) entry.getValue();
            @SuppressWarnings("unchecked")
            List<Map<String, String>> otelDataEntries = (List<Map<String, String>>) fieldData.get("otel");
            if (otelDataEntries != null) {
                for (Map<String, String> otelData : otelDataEntries) {
                    String relation = otelData.get("relation");
                    if ("match".equals(relation)) {
                        ecsToOTelAttributeNames.put(ecsName, ecsName);
                    } else if ("equivalent".equals(relation)) {
                        String attribute = otelData.get("attribute");
                        if (attribute != null) {
                            ecsToOTelAttributeNames.put(ecsName, attribute);
                        }
                    }
                }
            }
            if (ecsName.startsWith(AGENT_FIELDS_PREFIX)) {
                // for now, we consider all agent.* fields as resource attributes, but this may change in the future
                ecsResourceFields.add(ecsName);
            }
        }
    }

    private static Map<String, Object> loadEcsFields() throws IOException, InterruptedException {
        try (HttpClient httpClient = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(ECS_FLAT_FILE_URL)).build();
            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            try (
                InputStream is = response.body();
                XContentParser parser = XContentFactory.xContent(XContentType.YAML).createParser(XContentParserConfiguration.EMPTY, is)
            ) {
                return parser.map();
            }
        }
    }
}
