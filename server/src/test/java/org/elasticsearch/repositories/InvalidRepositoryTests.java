/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class InvalidRepositoryTests extends ESTestCase {

    private InvalidRepository repository = new InvalidRepository(
        randomProjectIdOrDefault(),
        new RepositoryMetadata("name", "type", Settings.EMPTY),
        new RepositoryException("name", "failed to create repository")
    );

    public void testShouldThrowWhenGettingMetadata() {
        final var expectedException = expectThrows(
            RepositoryException.class,
            () -> repository.getSnapshotGlobalMetadata(new SnapshotId("name", "uuid"))
        );
        assertThat(expectedException.getMessage(), equalTo("[name] repository type [type] failed to create on current node"));
        assertThat(expectedException.getCause(), isA(RepositoryException.class));
        assertThat(expectedException.getCause().getMessage(), equalTo("[name] failed to create repository"));
    }
}
