/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Tests getting the status of a running async search aggressively while
 * the search is running. In the past this has caused trouble with misbehaving
 * aggregations that can't call {@link Writeable#writeTo} concurrently with
 * {@link ToXContent#toXContent}.
 */
public class FinishAndGetRaceIT extends ESRestTestCase {
    public void testTermsCardinality() throws IOException {
        for (int i = 0; i < 1000; i++) {
            StringBuilder b = new StringBuilder();
            for (int j = 0; j < 1000; j++) {
                b.append("{\"index\": {}}\n");
                b.append("{\"i\": ").append(i).append(", \"j\": ").append(j).append("}\n");
            }
            Request bulk = new Request("POST", "/test/_bulk");
            bulk.setJsonEntity(b.toString());
            Map<String, Object> r = entityAsMap(client().performRequest(bulk));
            assertMap(r, matchesMap().extraOk().entry("errors", false));
        }
        fail("ADFADF");
    }
}
