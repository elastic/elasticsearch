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
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

public class GoldenIndexTests extends ESTestCase {

    public void testDescriptorIsForTheIndexAndWritesWithTheOfficialOrigin() {
        SystemIndexDescriptor descriptor = GoldenIndex.descriptor();

        assertThat(descriptor.getPrimaryIndex(), equalTo(".query_golden"));
        assertTrue(descriptor.matchesIndexPattern(".query_golden"));
        assertThat(descriptor.getOrigin(), equalTo(ClientHelper.QUERY_SAMPLING_ORIGIN));
    }

    public void testTheTwoIndicesDoNotOverlap() {
        // registering the descriptors of a plugin together is what fails when the patterns of two of them overlap
        new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "query_sampling",
                    "the indices of the query sampling",
                    List.of(QuerySamplingIndex.descriptor(), GoldenIndex.descriptor())
                )
            )
        );

        assertFalse(QuerySamplingIndex.descriptor().matchesIndexPattern(GoldenIndex.NAME));
        assertFalse(GoldenIndex.descriptor().matchesIndexPattern(QuerySamplingIndex.NAME));
    }

    @SuppressWarnings("unchecked") // the mappings are known to be nested maps, they are built in the class under test
    public void testMappingsAreStrictAndKeepTheLargeObjectsOutOfTheIndex() {
        Map<String, Object> root = XContentHelper.convertToMap(BytesReference.bytes(GoldenIndex.mappings()), false, XContentType.JSON).v2();
        Map<String, Object> mapping = (Map<String, Object>) root.get("_doc");

        assertThat(mapping, hasEntry("dynamic", "strict"));
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        for (String field : new String[] { "kind", "dataset_version", "completed", "fingerprint", "query", "live_hits", "ground_truth" }) {
            assertThat(properties, hasKey(field));
        }
        for (String field : new String[] { "query", "live_hits", "ground_truth" }) {
            assertThat((Map<String, Object>) properties.get(field), hasEntry("enabled", false));
        }
    }
}
