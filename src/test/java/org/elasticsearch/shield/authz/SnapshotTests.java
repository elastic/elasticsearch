/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.cluster.metadata.SnapshotMetaData;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.junit.Test;

import java.io.File;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * test to check if snapshotting and displaying of current snapshots work
 * can be removed after 1.4.3 is released as this is included
 *
 */
public class SnapshotTests extends ShieldIntegrationTest {

    // TODO remove after upgrading to 1.4.3
    @Test
    public void testThatSnapshottingWorks() throws Exception {
        File repositoryLocation = newTempDir();
        assertAcked(client().admin().cluster().preparePutRepository("my-repo").setType("fs").setSettings(ImmutableMap.of("location", repositoryLocation.getAbsolutePath())));

        client().prepareIndex("foo", "bar", "1").setSource("foo", "bar").get();
        client().prepareIndex("foo", "bar", "2").setSource("foo", "bar").get();
        client().prepareIndex("foo", "bar", "3").setSource("foo", "bar").get();
        client().prepareIndex("foo", "bar", "4").setSource("foo", "bar").get();
        client().prepareIndex("foo", "bar", "5").setSource("foo", "bar").get();
        refresh();

        // must be started async otherwise the code relevent path in TransportSnapshotsStatusAction about currently running snapshots is not executed
        client().admin().cluster().prepareCreateSnapshot("my-repo", "my-snapshot").setIndices("foo").get();

        SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus("my-repo").setSnapshots("my-snapshot").get();
        assertThat(snapshotsStatusResponse.getSnapshots(), hasSize(1));
        SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
        assertThat(snapshotStatus.getState(), anyOf(is(SnapshotMetaData.State.STARTED), is(SnapshotMetaData.State.SUCCESS)));

        assertAcked(client().admin().cluster().prepareDeleteSnapshot("my-repo", "my-snapshot").get());

        assertAcked(client().admin().cluster().prepareDeleteRepository("my-repo"));
    }

}
