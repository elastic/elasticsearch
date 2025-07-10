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

package co.elastic.elasticsearch.stateless.objectstore;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;

import static org.hamcrest.Matchers.equalTo;

@TestIssueLogging(
    issueUrl = "https://github.com/elastic/elasticsearch-serverless/issues/4161",
    value = "co.elastic.elasticsearch.serverless.multiproject.MultiProjectFileSettingsService:DEBUG"
)
@LuceneTestCase.SuppressFileSystems(value = { "ExtrasFS" })
public class FsMultiProjectObjectStoreIT extends FsObjectStoreTests {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected Settings projectSettings(ProjectId projectId) {
        return Settings.builder()
            .put("stateless.object_store.type", "fs")
            .put("stateless.object_store.bucket", "project_" + projectId)
            .put("stateless.object_store.base_path", "base_path")
            .build();
    }

    @Override
    protected Settings projectSecrets(ProjectId projectId) {
        return Settings.EMPTY;
    }

    @Override
    protected Settings repositorySettings(ProjectId projectId) {
        return Settings.builder().put(super.repositorySettings()).put("location", "backup_" + projectId.id()).build();
    }

    @Override
    protected void assertBackupRepositorySettings(RepositoryMetadata repositoryMetadata, ProjectId projectId) {
        assertThat(repositoryMetadata.settings().get("location"), equalTo("backup_" + projectId.id()));
    }
}
