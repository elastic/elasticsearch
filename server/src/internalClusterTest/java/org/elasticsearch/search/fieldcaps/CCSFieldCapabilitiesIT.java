/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fieldcaps;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CCSFieldCapabilitiesIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("remote_cluster");
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testFailuresFromRemote() throws IOException {
        Settings indexSettings = Settings.builder().put("index.number_of_replicas", 0).build();
        final Client localClient = client(LOCAL_CLUSTER);
        final Client remoteClient = client("remote_cluster");
        String localIndex = "local_test";
        assertAcked(localClient.admin().indices().prepareCreate(localIndex).setSettings(indexSettings));
        localClient.prepareIndex(localIndex).setId("1").setSource("foo", "bar").get();
        localClient.admin().indices().prepareRefresh(localIndex).get();

        String remoteErrorIndex = "remote_test_error";
        assertAcked(remoteClient.admin().indices().prepareCreate(remoteErrorIndex).setSettings(indexSettings));
        remoteClient.prepareIndex(remoteErrorIndex).setId("2").setSource("foo", "bar").get();
        remoteClient.admin().indices().prepareRefresh(remoteErrorIndex).get();

        // regular field_caps across clusters
        FieldCapabilitiesResponse response = client().prepareFieldCaps("*", "remote_cluster:*").setFields("*").get();
        assertThat(Arrays.asList(response.getIndices()), containsInAnyOrder(localIndex, "remote_cluster:" + remoteErrorIndex));

        // Closed shards will result to index error because shards must be in readable state
        FieldCapabilitiesIT.closeShards(cluster("remote_cluster"), remoteErrorIndex);

        response = client().prepareFieldCaps("*", "remote_cluster:*").setFields("*").get();
        assertThat(response.getIndices()[0], equalTo(localIndex));
        assertThat(response.getFailedIndicesCount(), equalTo(1));
        FieldCapabilitiesFailure failure = response.getFailures()
            .stream()
            .filter(f -> Arrays.asList(f.getIndices()).contains("remote_cluster:*"))
            .findFirst()
            .get();
        Exception ex = failure.getException();
        assertEquals(RemoteTransportException.class, ex.getClass());
        Throwable cause = ExceptionsHelper.unwrapCause(ex);
        assertEquals(IllegalIndexShardStateException.class, cause.getClass());
        assertEquals(
            "CurrentState[CLOSED] operations only allowed when shard state is one of [POST_RECOVERY, STARTED]",
            cause.getMessage()
        );

        // if we only query the remote we should get back an exception only
        ex = expectThrows(IllegalIndexShardStateException.class, client().prepareFieldCaps("remote_cluster:*").setFields("*"));
        assertEquals("CurrentState[CLOSED] operations only allowed when shard state is one of [POST_RECOVERY, STARTED]", ex.getMessage());

        // add an index that doesn't fail to the remote
        assertAcked(remoteClient.admin().indices().prepareCreate("okay_remote_index"));
        remoteClient.prepareIndex("okay_remote_index").setId("2").setSource("foo", "bar").get();
        remoteClient.admin().indices().prepareRefresh("okay_remote_index").get();

        response = client().prepareFieldCaps("*", "remote_cluster:*").setFields("*").get();
        assertThat(Arrays.asList(response.getIndices()), containsInAnyOrder(localIndex, "remote_cluster:okay_remote_index"));
        assertThat(response.getFailedIndicesCount(), equalTo(1));
        failure = response.getFailures()
            .stream()
            .filter(f -> Arrays.asList(f.getIndices()).contains("remote_cluster:" + remoteErrorIndex))
            .findFirst()
            .get();
        ex = failure.getException();
        assertEquals(IllegalIndexShardStateException.class, ex.getClass());
        assertEquals("CurrentState[CLOSED] operations only allowed when shard state is one of [POST_RECOVERY, STARTED]", ex.getMessage());
    }

    public void testFailedToConnectToRemoteCluster() throws Exception {
        String localIndex = "local_index";
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate(localIndex));
        client(LOCAL_CLUSTER).prepareIndex(localIndex).setId("1").setSource("foo", "bar").get();
        client(LOCAL_CLUSTER).admin().indices().prepareRefresh(localIndex).get();
        cluster("remote_cluster").close();
        FieldCapabilitiesResponse response = client().prepareFieldCaps("*", "remote_cluster:*").setFields("*").get();
        assertThat(response.getIndices(), arrayContaining(localIndex));
        List<FieldCapabilitiesFailure> failures = response.getFailures();
        assertThat(failures, hasSize(1));
        assertThat(failures.get(0).getIndices(), arrayContaining("remote_cluster:*"));
    }
}
