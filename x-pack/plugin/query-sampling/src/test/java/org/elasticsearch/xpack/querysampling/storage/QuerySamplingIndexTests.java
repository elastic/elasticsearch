/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

public class QuerySamplingIndexTests extends ESTestCase {

    public void testDescriptorIsForTheIndexAndWritesWithTheOfficialOrigin() {
        // building the descriptor validates its mappings, in particular that they carry a version
        SystemIndexDescriptor descriptor = QuerySamplingIndex.descriptor();

        assertThat(descriptor.getPrimaryIndex(), equalTo(".query_sampling"));
        assertTrue(descriptor.matchesIndexPattern(".query_sampling"));
        assertThat(descriptor.getOrigin(), equalTo(ClientHelper.QUERY_SAMPLING_ORIGIN));
    }

    @SuppressWarnings("unchecked") // the mappings are known to be nested maps, they are built in the class under test
    public void testMappingsAreStrictAndListEveryStoredField() {
        Map<String, Object> root = XContentHelper.convertToMap(
            BytesReference.bytes(QuerySamplingIndex.mappings()),
            false,
            XContentType.JSON
        ).v2();
        Map<String, Object> mapping = (Map<String, Object>) root.get("_doc");

        assertThat(mapping, hasEntry("dynamic", "strict"));
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        for (String field : new String[] {
            "sampler_id",
            "fingerprint",
            "indices",
            "field",
            "k",
            "multiplicity",
            "weighted_multiplicity",
            "inclusion_probability",
            "seen_probability",
            "capture_rate",
            "picked_at",
            "updated_at",
            "has_ground_truth",
            "query",
            "live_hits",
            "ground_truth" }) {
            assertThat(properties, hasKey(field));
        }
        assertThat((Map<String, Object>) properties.get("query"), hasEntry("enabled", false));
    }
}
