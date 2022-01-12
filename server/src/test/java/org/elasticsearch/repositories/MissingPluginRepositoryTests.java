/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

public class MissingPluginRepositoryTests extends ESTestCase {

    private MissingPluginRepository repository = new MissingPluginRepository(new RepositoryMetadata("name", "type", Settings.EMPTY));

    public void testShouldThrowWhenGettingMetadata() {
        expectThrows(RepositoryPluginException.class, () -> repository.getSnapshotGlobalMetadata(new SnapshotId("name", "uuid")));
    }

    public void testShouldNotThrowWhenApplyingLifecycleChanges() {
        repository.start();
        repository.stop();
    }

    public void testShouldNotThrowWhenClosingToAllowRemovingRepo() {
        repository.close();
    }
}
