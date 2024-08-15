/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class NativePrivilegeStoreRetrySingleNodeTests extends SecuritySingleNodeTestCase {

    private static final int MAX_RETRIES = 10;

    @BeforeClass
    public static void setup() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            System.setProperty("es.xpack.security.authz.store.get_privileges.max_retries", String.valueOf(MAX_RETRIES));
            return null;
        });
    }

    @AfterClass
    public static void teardown() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            System.clearProperty("es.xpack.security.authz.store.get_privileges.max_retries");
            return null;
        });
    }

    @Before
    public void configureApplicationPrivileges() {
        final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest();
        final List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp-1", "read", Set.of("action:read"), emptyMap()),
            new ApplicationPrivilegeDescriptor("yourapp-1", "read", Set.of("action:read"), emptyMap())
        );
        putPrivilegesRequest.setPrivileges(applicationPrivilegeDescriptors);
        client().execute(PutPrivilegesAction.INSTANCE, putPrivilegesRequest).actionGet();
    }

    public void testRetry() {
        // TODO this test is very brittle right now -- it marks a shard as failed (which is only transient, and the shard auto-recovers)
        // then attempts to get privileges
        // this (on average) tests that retry works, since usually the shard has not recovered yet by the time we run the privilege request
        // however there are race conditions both ways: the shard might take longer to recover than the retry, or conversely recover faster
        // than the first request comes in
        // we should make this more robust

        // TODO make this work with the main security index alias
        String indexName = ".security-7";
        failShardOnIndex(indexName);

        GetPrivilegesRequest request = new GetPrivilegesRequest();
        request.application("myapp-*");
        GetPrivilegesResponse actual = client().execute(GetPrivilegesAction.INSTANCE, request).actionGet();
        assertThat(actual.privileges(), is(not(emptyArray())));
    }

    private void failShardOnIndex(String indexName) {
        ClusterState state = clusterAdmin().prepareState().get().getState();
        Index securityIndex = state.metadata().index(indexName).getIndex();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(securityIndex);
        IndexShard shard = indexService.getShard(0);
        // This induces a shard failure -- the shard eventually gets marked as stale and restarted, resulting in successful responses
        // resuming
        shard.failShard("testing retries", new UnavailableShardsException(shard.shardId(), "bad shard"));
    }
}
