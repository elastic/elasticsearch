/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryStats;

import static org.hamcrest.Matchers.anEmptyMap;

public class FsObjectStoreTests extends AbstractObjectStoreIntegTestCase {

    @Override
    protected String repositoryType() {
        return "fs";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder().put(super.repositorySettings()).put("location", randomRepoPath()).build();
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats repositoryStats, boolean withRandomCrud, OperationPurpose purpose) {
        assertThat(repositoryStats.actionStats, anEmptyMap());
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats) {
        assertThat(repositoryStats.actionStats, anEmptyMap());
    }
}
