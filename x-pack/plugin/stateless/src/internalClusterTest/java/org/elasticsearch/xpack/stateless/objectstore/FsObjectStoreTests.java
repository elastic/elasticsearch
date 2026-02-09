/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
