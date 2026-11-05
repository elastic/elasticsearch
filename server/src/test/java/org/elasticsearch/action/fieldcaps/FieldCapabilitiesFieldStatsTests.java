/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class FieldCapabilitiesFieldStatsTests extends ESSingleNodeTestCase {

    public void testFieldStatsFromTermsAndPoints() {
        createIndex(
            "idx",
            Settings.builder().put("index.number_of_shards", 1).build(),
            "k",
            "type=keyword",
            "n",
            "type=long",
            "d",
            "type=date",
            "t",
            "type=text"
        );
        prepareIndex("idx").setSource(Map.of("k", "abc", "n", 5, "d", "2020-01-01")).get();
        prepareIndex("idx").setSource(Map.of("k", List.of("de", "fgh"), "n", -3)).get();
        prepareIndex("idx").setSource(Map.of("t", "only text")).get();
        client().admin().indices().prepareRefresh("idx").get();

        Map<String, IndexFieldStats> stats = fieldStats(true);
        // every segment's terms fit in the length sample, so the byte total is exact
        assertThat(stats.get("k"), equalTo(new IndexFieldStats(2, 3, 8, null, null)));
        assertThat(stats.get("n"), equalTo(new IndexFieldStats(2, 2, -1, -3L, 5L)));
        assertThat(stats.get("d"), equalTo(new IndexFieldStats(1, 1, -1, 1577836800000L, 1577836800000L)));
        assertNull("text fields have no cheap stats", stats.get("t"));

        // the field caps cache must not answer a request without stats with a cached response that has them, or vice versa
        assertThat(fieldStats(false), anEmptyMap());
        assertThat(fieldStats(true), not(anEmptyMap()));
    }

    private Map<String, IndexFieldStats> fieldStats(boolean includeFieldStats) {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices("idx")
            .fields("k", "n", "d", "t")
            .includeFieldStats(includeFieldStats);
        request.setMergeResults(false);
        FieldCapabilitiesResponse response = client().fieldCaps(request).actionGet();
        assertThat(response.getIndexResponses(), hasSize(1));
        return response.getIndexResponses().get(0).getFieldStats();
    }
}
