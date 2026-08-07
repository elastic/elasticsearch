/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class EnrichStoreCrudTests extends AbstractEnrichTestCase {

    private final ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;

    public void testCrud() throws Exception {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        EnrichPolicy result = EnrichStore.getPolicy(name, clusterService.state().metadata().getProject(projectId));
        assertThat(result, equalTo(policy));

        Map<String, EnrichPolicy> listPolicies = EnrichStore.getPolicies(clusterService.state().metadata().getProject(projectId));
        assertThat(listPolicies.size(), equalTo(1));
        assertThat(listPolicies.get(name), equalTo(policy));

        deleteEnrichPolicy(name, clusterService);
        result = EnrichStore.getPolicy(name, clusterService.state().metadata().getProject(projectId));
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
        EnrichPolicy result = EnrichStore.getPolicy(name, clusterService.state().metadata().getProject(projectId));
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
            assertThat(
                error.getMessage(),
                equalTo("unsupported policy type [unsupported_type], supported types are [match, geo_match, range]")
            );
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
            () -> EnrichStore.getPolicy(nullOrEmptyName, clusterService.state().metadata().getProject(projectId))
        );

        assertThat(error.getMessage(), equalTo("name is missing or empty"));

        EnrichPolicy policy = EnrichStore.getPolicy("null-policy", clusterService.state().metadata().getProject(projectId));
        assertNull(policy);
    }

    public void testListValidation() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        Map<String, EnrichPolicy> policies = EnrichStore.getPolicies(clusterService.state().metadata().getProject(projectId));
        assertTrue(policies.isEmpty());
    }

    public void testMaxPolicies() throws Exception {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();

        // With a limit of one, the first policy can be stored.
        EnrichPolicy policy1 = randomEnrichPolicy(XContentType.JSON);
        createSourceIndices(policy1);
        AtomicReference<Exception> error = putPolicyWithLimits("policy-1", policy1, 1, ByteSizeValue.ofGb(1), clusterService, resolver);
        assertThat(error.get(), nullValue());

        // A second policy is rejected because the limit has already been reached.
        EnrichPolicy policy2 = randomEnrichPolicy(XContentType.JSON);
        createSourceIndices(policy2);
        error = putPolicyWithLimits("policy-2", policy2, 1, ByteSizeValue.ofGb(1), clusterService, resolver);
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("maximum number of enrich policies [1]"));
        assertThat(error.get().getMessage(), containsString("enrich.max_policies"));

        deleteEnrichPolicy("policy-1", clusterService);
    }

    public void testMaxTotalMetadataSize() throws Exception {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();

        EnrichPolicy policy1 = randomEnrichPolicy(XContentType.JSON);
        createSourceIndices(policy1);
        // A generous count limit but a tiny total-size budget: the first policy fits, the second pushes the aggregate over the budget.
        long firstPolicySize = policy1.serializedSizeInBytes();
        AtomicReference<Exception> error = putPolicyWithLimits(
            "policy-1",
            policy1,
            1000,
            ByteSizeValue.ofBytes(firstPolicySize + 10),
            clusterService,
            resolver
        );
        assertThat(error.get(), nullValue());

        EnrichPolicy policy2 = randomEnrichPolicy(XContentType.JSON);
        createSourceIndices(policy2);
        error = putPolicyWithLimits("policy-2", policy2, 1000, ByteSizeValue.ofBytes(firstPolicySize + 10), clusterService, resolver);
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("total size of all enrich policies"));
        assertThat(error.get().getMessage(), containsString("enrich.max_total_metadata_size"));

        deleteEnrichPolicy("policy-1", clusterService);
    }

    private AtomicReference<Exception> putPolicyWithLimits(
        String name,
        EnrichPolicy policy,
        int maxPolicies,
        ByteSizeValue maxTotalSize,
        ClusterService clusterService,
        IndexNameExpressionResolver resolver
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        EnrichStore.putPolicy(projectId, name, policy, maxPolicies, maxTotalSize, clusterService, resolver, e -> {
            error.set(e);
            latch.countDown();
        });
        latch.await();
        return error;
    }

}
