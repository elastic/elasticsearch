/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fieldcaps;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fieldcaps.FieldCapabilitiesIT.ExceptionOnRewriteQueryBuilder;
import org.elasticsearch.search.fieldcaps.FieldCapabilitiesIT.ExceptionOnRewriteQueryPlugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class CCSFieldCapabilitiesIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("remote_cluster");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(ExceptionOnRewriteQueryPlugin.class);
        return plugins;
    }

    public void testFailuresFromRemote() {
        final Client localClient = client(LOCAL_CLUSTER);
        final Client remoteClient = client("remote_cluster");
        String localIndex = "local_test";
        assertAcked(localClient.admin().indices().prepareCreate(localIndex));
        localClient.prepareIndex(localIndex).setId("1").setSource("foo", "bar").get();
        localClient.admin().indices().prepareRefresh(localIndex).get();

        String remoteIndex = "remote_test_error";
        assertAcked(remoteClient.admin().indices().prepareCreate(remoteIndex));
        remoteClient.prepareIndex(remoteIndex).setId("2").setSource("foo", "bar").get();
        remoteClient.admin().indices().prepareRefresh(remoteIndex).get();

        // regular field_caps across clusters
        FieldCapabilitiesResponse response = client().prepareFieldCaps("*", "remote_cluster:*").setFields("*").get();
        assertThat(Arrays.asList(response.getIndices()), containsInAnyOrder(localIndex, "remote_cluster:" + remoteIndex));

        // adding an index filter so remote call should fail
        response = client().prepareFieldCaps("*", "remote_cluster:*")
            .setFields("*")
            .setIndexFilter(new ExceptionOnRewriteQueryBuilder())
            .get();
        assertThat(response.getIndices()[0], equalTo(localIndex));
        assertThat(response.getFailedIndices()[0], equalTo("remote_cluster:" + remoteIndex));
        Exception failure = response.getFailures().get("remote_cluster:" + remoteIndex);
        assertEquals(RemoteTransportException.class, failure.getClass());
        assertEquals(IllegalArgumentException.class, failure.getCause().getClass());
        assertEquals("I throw because I choose to.", failure.getCause().getMessage());
    }
}
