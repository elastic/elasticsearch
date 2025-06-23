/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class UnknownTypeRepositoryTests extends ESTestCase {

    private ProjectId projectId = randomProjectIdOrDefault();
    private UnknownTypeRepository repository = new UnknownTypeRepository(projectId, new RepositoryMetadata("name", "type", Settings.EMPTY));

    public void testShouldThrowWhenGettingMetadata() {
        assertThat(repository.getProjectId(), equalTo(projectId));
        expectThrows(RepositoryException.class, () -> repository.getSnapshotGlobalMetadata(new SnapshotId("name", "uuid")));
    }

    public void testShouldNotThrowWhenApplyingLifecycleChanges() {
        repository.start();
        repository.stop();
    }

    public void testShouldNotThrowWhenClosingToAllowRemovingRepo() {
        repository.close();
    }
}
