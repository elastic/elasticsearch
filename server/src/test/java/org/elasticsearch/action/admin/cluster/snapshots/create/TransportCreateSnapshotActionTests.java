/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.snapshots.SnapshotEncryptedDataFeatures;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TransportCreateSnapshotActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private FeatureService featureService;
    private TransportCreateSnapshotAction action;

    @Before
    public void initializeComponents() {
        threadPool = new TestThreadPool(TransportCreateSnapshotActionTests.class.getName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        transportService = new CapturingTransport().createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> clusterService.localNode(),
            clusterService.getClusterSettings(),
            Set.of()
        );
        featureService = new FeatureService(List.of(new SnapshotEncryptedDataFeatures()));
        action = new TransportCreateSnapshotAction(
            transportService,
            clusterService,
            threadPool,
            null, // SnapshotsService: requests rejected by the feature check never reach the master operation
            ActionFilters.EMPTY,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            featureService
        );
    }

    @After
    public void shutdownComponents() {
        threadPool.shutdown();
        transportService.close();
        clusterService.close();
    }

    public void testRejectsEncryptedDataUntilAllNodesSupportIt() {
        assumeTrue("snapshot_encrypted_data feature flag must be enabled", SnapshotEncryptedData.FEATURE_FLAG.isEnabled());
        assertFalse(featureService.clusterHasFeature(clusterService.state(), SnapshotEncryptedDataFeatures.CREATE_ENCRYPTED_DATA));
        final CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap").encryptedData(
            new SnapshotEncryptedData(
                SnapshotEncryptedData.TYPE_PASSWORD,
                new SecureString("correct-horse-battery-staple".toCharArray()),
                null
            )
        );

        final IllegalArgumentException e = safeAwaitFailure(
            IllegalArgumentException.class,
            CreateSnapshotResponse.class,
            l -> ActionTestUtils.execute(action, request, l)
        );

        assertThat(e.getMessage(), equalTo("[encrypted_data] is not supported until all nodes are upgraded"));
    }
}
