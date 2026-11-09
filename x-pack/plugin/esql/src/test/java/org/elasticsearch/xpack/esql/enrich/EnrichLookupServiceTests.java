/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;
import java.util.Map;

public class EnrichLookupServiceTests extends ESTestCase {

    private static final Map<String, EnrichPolicy> POLICIES = Map.of("users", policy(), "multi-part-name", policy());

    public void testIndexOfExistingPolicyIsAllowed() {
        assertTrue(isEnrichPolicyIndex(EnrichPolicy.getIndexName("users", randomNonNegativeLong()), metaFor("users")));
        assertTrue(isEnrichPolicyIndex(EnrichPolicy.getIndexName("multi-part-name", randomNonNegativeLong()), metaFor("multi-part-name")));
    }

    public void testMigratedIndexOfExistingPolicyIsAllowed() {
        String index = EnrichPolicy.getIndexName("users", randomNonNegativeLong()) + SystemIndices.UPGRADED_INDEX_SUFFIX;
        assertTrue(isEnrichPolicyIndex(index, metaFor("users")));
        assertTrue(isEnrichPolicyIndex(index + "-reindexed-for-11", metaFor("users")));
    }

    public void testRegularIndexIsRejected() {
        assertFalse(isEnrichPolicyIndex("users", metaFor("users")));
        assertFalse(isEnrichPolicyIndex("users-1700000000000", metaFor("users")));
    }

    public void testSystemIndexIsRejected() {
        assertFalse(isEnrichPolicyIndex(".security-7", metaFor("users")));
        assertFalse(isEnrichPolicyIndex(".kibana_1", "{}"));
    }

    public void testEnrichIndexWithoutPolicyNameIsRejected() {
        assertFalse(isEnrichPolicyIndex(".enrich-users-1700000000000", "{}"));
        assertFalse(isEnrichPolicyIndex(".enrich-users-1700000000000", "{\"_meta\":{}}"));
        assertFalse(isEnrichPolicyIndex(".enrich-users-1700000000000", "{\"_meta\":{\"enrich_policy_name\":5}}"));
    }

    public void testEnrichIndexOfUnknownPolicyIsRejected() {
        assertFalse(isEnrichPolicyIndex(".enrich-users-1700000000000", metaFor("other")));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(Map.of(), indexMetadata(".enrich-users-1700000000000", metaFor("users"))));
    }

    public void testIndexWithoutMappingIsRejected() {
        IndexMetadata index = IndexMetadata.builder(".enrich-users-1700000000000")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .build();
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, index));
    }

    public void testMissingIndexIsRejected() {
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, null));
    }

    private static boolean isEnrichPolicyIndex(String index, String mapping) {
        return EnrichLookupService.isEnrichPolicyIndex(POLICIES, indexMetadata(index, mapping));
    }

    private static IndexMetadata indexMetadata(String index, String mapping) {
        return IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mapping).build();
    }

    private static String metaFor(String policyName) {
        return "{\"_meta\":{\"" + EnrichPolicy.ENRICH_POLICY_NAME_FIELD_NAME + "\":\"" + policyName + "\"}}";
    }

    private static EnrichPolicy policy() {
        return new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source"), "key", List.of("value"));
    }
}
