/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.recovery;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.recovery.AbstractIndexRecoveryIntegTestCase;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexRecoveryWithSnapshotsIT extends AbstractIndexRecoveryIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ConfigurableMockSnapshotBasedRecoveriesPlugin.class);
    }

    public void testTransientErrorsDuringRecoveryAreRetried() throws Exception {
        checkTransientErrorsDuringRecoveryAreRetried(PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT);
    }

    public void testDisconnectsWhileRecovering() throws Exception {
        checkDisconnectsWhileRecovering(PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT);
    }

    public void testDisconnectsDuringRecovery() throws Exception {
        checkDisconnectsDuringRecovery(true);
    }
}
