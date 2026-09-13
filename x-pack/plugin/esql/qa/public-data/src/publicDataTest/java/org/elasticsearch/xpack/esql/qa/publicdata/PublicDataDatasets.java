/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Idempotent dataset registration for this suite, mirroring {@link DatasetRegistry}'s
 * signature-cache semantics but additionally carrying the optional declared-schema
 * {@code mappings} block, which {@code DatasetRegistry.ensureDataset} does not model (and that
 * class is shared infrastructure this project deliberately does not modify). Data sources still
 * go through {@link DatasetRegistry#ensureDataSource}; call {@link #cleanup} <em>before</em>
 * {@code DatasetRegistry.cleanup} so datasets are gone before their parent data sources.
 */
final class PublicDataDatasets {

    /** dataset name -> content signature of the last successful PUT. */
    private static final Map<String, String> datasets = new LinkedHashMap<>();

    private PublicDataDatasets() {}

    static synchronized void ensureDataset(
        RestClient client,
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings,
        Map<String, Object> mappings
    ) throws IOException {
        String signature = dataSource + "|" + resource + "|" + settings + "|" + mappings;
        if (signature.equals(datasets.get(name))) {
            return;
        }
        Request request = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder body = jsonBuilder()) {
            body.startObject().field("data_source", dataSource).field("resource", resource);
            if (settings.isEmpty() == false) {
                body.field("settings", settings);
            }
            if (mappings.isEmpty() == false) {
                body.field("mappings", mappings);
            }
            body.endObject();
            request.setJsonEntity(Strings.toString(body));
        }
        client.performRequest(request);
        datasets.put(name, signature);
    }

    /** Deletes every dataset this helper registered and clears the cache. */
    static synchronized void cleanup(RestClient client) throws IOException {
        try {
            for (String name : datasets.keySet()) {
                DatasetRegistry.deleteIgnoringMissing(client, "/_query/dataset/" + name);
            }
        } finally {
            datasets.clear();
        }
    }

    /** Clears the cache without REST calls, for suites that could not run {@link #cleanup}. */
    static synchronized void clearCaches() {
        datasets.clear();
    }
}
