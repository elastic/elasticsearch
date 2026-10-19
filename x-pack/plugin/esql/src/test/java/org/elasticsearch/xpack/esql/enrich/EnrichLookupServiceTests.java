/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;
import java.util.Map;

public class EnrichLookupServiceTests extends ESTestCase {

    private static final Map<String, EnrichPolicy> POLICIES = Map.of("users", policy(), "multi-part-name", policy());

    public void testConcreteIndexOfExistingPolicyIsAllowed() {
        assertTrue(EnrichLookupService.isEnrichPolicyIndex(POLICIES, EnrichPolicy.getIndexName("users", randomNonNegativeLong())));
        assertTrue(
            EnrichLookupService.isEnrichPolicyIndex(POLICIES, EnrichPolicy.getIndexName("multi-part-name", randomNonNegativeLong()))
        );
    }

    public void testRegularIndexIsRejected() {
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, "users"));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, "users-1700000000000"));
    }

    public void testSystemIndexIsRejected() {
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, ".security-7"));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, ".kibana_1"));
    }

    public void testEnrichIndexOfUnknownPolicyIsRejected() {
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, EnrichPolicy.getIndexName("other", randomNonNegativeLong())));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, EnrichPolicy.getIndexName("name", randomNonNegativeLong())));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(Map.of(), EnrichPolicy.getIndexName("users", randomNonNegativeLong())));
    }

    public void testEnrichAliasIsRejected() {
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, EnrichPolicy.getBaseName("users")));
    }

    public void testMalformedEnrichIndexNameIsRejected() {
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, ".enrich-users-"));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, ".enrich-users-12a4"));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, ".enrich-users-123-"));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, ".enrich--123"));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, ".enrich-"));
    }

    public void testIndexThatOnlyLooksLikeEnrichIsRejected() {
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, "Xenrich-users-123"));
        assertFalse(EnrichLookupService.isEnrichPolicyIndex(POLICIES, "enrich-users-123"));
    }

    private static EnrichPolicy policy() {
        return new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source"), "key", List.of("value"));
    }
}
