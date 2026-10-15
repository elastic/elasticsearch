/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

abstract class QuerySamplingRestTestCase extends ESRestTestCase {

    void setUpIndexAndSampling() throws IOException {
        createIndex("vectors", indexSettings(1, 0).build(), """
            "properties": { "vec": { "type": "dense_vector", "dims": 2, "index": true, "similarity": "l2_norm" } }
            """);
        for (int i = 0; i < 5; i++) {
            Request index = new Request("PUT", "/vectors/_doc/" + i);
            index.setJsonEntity("{\"vec\": [" + i + ", " + i + "]}");
            client().performRequest(index);
        }
        client().performRequest(new Request("POST", "/vectors/_refresh"));

        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.enabled": true, "xpack.query_sampling.capture_rate": 1.0 } }
            """);
        client().performRequest(settings);
    }

    float sampleOneQuery() throws Exception {
        List<Float> sent = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            float x = randomFloat();
            sent.add(x);
            client().performRequest(knnSearch(x));
        }
        Float[] sampled = new Float[1];
        assertBusy(() -> {
            for (float x : sent) {
                if (storedValue(x, "weighted_multiplicity") != null) {
                    sampled[0] = x;
                    return;
                }
            }
            fail("none of the searched queries is in the index of the sample");
        });
        return sampled[0];
    }

    static Request knnSearch(float x) {
        Request search = new Request("POST", "/vectors/_search");
        search.setJsonEntity("{ \"knn\": { \"field\": \"vec\", \"query_vector\": [" + x + ", 1.0], \"k\": 3, \"num_candidates\": 10 } }");
        return search;
    }

    static Object storedValue(float x, String path) throws IOException {
        if (refreshSampleIndex() == false) {
            return null;
        }
        Request search = new Request("GET", "/.query_sampling/_search?size=10000");
        search.setOptions(systemIndexAccess());
        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(search));
        List<?> hits = result.evaluate("hits.hits");
        for (int i = 0; i < hits.size(); i++) {
            double first = ((Number) result.evaluate("hits.hits." + i + "._source.query.query_vector.0")).doubleValue();
            if (Math.abs(first - x) < 1e-6) {
                return result.evaluate("hits.hits." + i + "._source." + path);
            }
        }
        return null;
    }

    static boolean refreshSampleIndex() throws IOException {
        Request refresh = new Request("POST", "/.query_sampling/_refresh");
        refresh.setOptions(systemIndexAccess());
        try {
            client().performRequest(refresh);
            return true;
        } catch (ResponseException e) {
            return false;
        }
    }

    static RequestOptions systemIndexAccess() {
        return RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build();
    }
}
