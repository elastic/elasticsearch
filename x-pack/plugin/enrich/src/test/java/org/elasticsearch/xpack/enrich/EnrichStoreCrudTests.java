/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichStoreCrudTests extends AbstractEnrichTestCase {
    public void testCrud() throws Exception {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        EnrichPolicy result = EnrichStore.getPolicy(name, clusterService.state());
        assertThat(result, equalTo(policy));

        Map<String, EnrichPolicy> listPolicies = EnrichStore.getPolicies(clusterService.state());
        assertThat(listPolicies.size(), equalTo(1));
        assertThat(listPolicies.get(name), equalTo(policy));

        deleteEnrichPolicy(name, clusterService);
        result = EnrichStore.getPolicy(name, clusterService.state());
        assertThat(result, nullValue());
    }

    public void testImmutability() throws Exception {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        error = saveEnrichPolicy(name, policy, clusterService);
        assertTrue(error.get().getMessage().contains("policy [my-policy] already exists"));
        ;

        deleteEnrichPolicy(name, clusterService);
        EnrichPolicy result = EnrichStore.getPolicy(name, clusterService.state());
        assertThat(result, nullValue());
    }

    public void testPutValidation() throws Exception {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        {
            String nullOrEmptyName = randomBoolean() ? "" : null;

            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> saveEnrichPolicy(nullOrEmptyName, policy, clusterService)
            );

            assertThat(error.getMessage(), equalTo("name is missing or empty"));
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> saveEnrichPolicy("my-policy", null, clusterService)
            );

            assertThat(error.getMessage(), equalTo("policy is missing"));
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> saveEnrichPolicy("my#policy", policy, clusterService)
            );
            assertThat(error.getMessage(), equalTo("Invalid policy name [my#policy], must not contain '#'"));
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> saveEnrichPolicy("..", policy, clusterService)
            );
            assertThat(error.getMessage(), equalTo("Invalid policy name [..], must not be '.' or '..'"));
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> saveEnrichPolicy("myPolicy", policy, clusterService)
            );
            assertThat(error.getMessage(), equalTo("Invalid policy name [myPolicy], must be lowercase"));
        }
        {
            EnrichPolicy invalidPolicy = new EnrichPolicy("unsupported_type", null, List.of("index"), "field", List.of("field"));
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> saveEnrichPolicy("name", invalidPolicy, clusterService)
            );
            assertThat(error.getMessage(), equalTo("unsupported policy type [unsupported_type], supported types are [match, geo_match]"));
        }
    }

    public void testDeleteValidation() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        {
            String nullOrEmptyName = randomBoolean() ? "" : null;

            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> deleteEnrichPolicy(nullOrEmptyName, clusterService)
            );

            assertThat(error.getMessage(), equalTo("name is missing or empty"));
        }
        {
            ResourceNotFoundException error = expectThrows(
                ResourceNotFoundException.class,
                () -> deleteEnrichPolicy("my-policy", clusterService)
            );

            assertThat(error.getMessage(), equalTo("policy [my-policy] not found"));
        }
    }

    public void testGetValidation() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String nullOrEmptyName = randomBoolean() ? "" : null;

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> EnrichStore.getPolicy(nullOrEmptyName, clusterService.state())
        );

        assertThat(error.getMessage(), equalTo("name is missing or empty"));

        EnrichPolicy policy = EnrichStore.getPolicy("null-policy", clusterService.state());
        assertNull(policy);
    }

    public void testListValidation() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        Map<String, EnrichPolicy> policies = EnrichStore.getPolicies(clusterService.state());
        assertTrue(policies.isEmpty());
    }

}
