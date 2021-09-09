/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.StoredScriptsIT;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertIndexTemplateExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertIndexTemplateMissing;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SnapshotCustomPluginStateIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestTestPlugin.class, StoredScriptsIT.CustomScriptPlugin.class);
    }

    public void testIncludeGlobalState() throws Exception {
        createRepository("test-repo", "fs");

        boolean testTemplate = randomBoolean();
        boolean testPipeline = randomBoolean();
        boolean testScript = (testTemplate == false && testPipeline == false) || randomBoolean(); // At least something should be stored

        if (testTemplate) {
            logger.info("-->  creating test template");
            assertThat(
                client().admin()
                    .indices()
                    .preparePutTemplate("test-template")
                    .setPatterns(Collections.singletonList("te*"))
                    .setMapping(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("_doc")
                            .startObject("properties")
                            .startObject("field1")
                            .field("type", "text")
                            .field("store", true)
                            .endObject()
                            .startObject("field2")
                            .field("type", "keyword")
                            .field("store", true)
                            .endObject()
                            .endObject()
                            .endObject()
                            .endObject()
                    )
                    .get()
                    .isAcknowledged(),
                equalTo(true)
            );
        }

        if (testPipeline) {
            logger.info("-->  creating test pipeline");
            BytesReference pipelineSource = BytesReference.bytes(
                jsonBuilder().startObject()
                    .field("description", "my_pipeline")
                    .startArray("processors")
                    .startObject()
                    .startObject("test")
                    .endObject()
                    .endObject()
                    .endArray()
                    .endObject()
            );
            assertAcked(clusterAdmin().preparePutPipeline("barbaz", pipelineSource, XContentType.JSON).get());
        }

        if (testScript) {
            logger.info("-->  creating test script");
            assertAcked(
                clusterAdmin().preparePutStoredScript()
                    .setId("foobar")
                    .setContent(
                        new BytesArray("{\"script\": { \"lang\": \"" + MockScriptEngine.NAME + "\", \"source\": \"1\"} }"),
                        XContentType.JSON
                    )
            );
        }

        logger.info("--> snapshot without global state");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-no-global-state")
            .setIndices()
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(getSnapshot("test-repo", "test-snap-no-global-state").state(), equalTo(SnapshotState.SUCCESS));
        SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo")
            .addSnapshots("test-snap-no-global-state")
            .get();
        assertThat(snapshotsStatusResponse.getSnapshots().size(), equalTo(1));
        SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
        assertThat(snapshotStatus.includeGlobalState(), equalTo(false));

        logger.info("--> snapshot with global state");
        createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-with-global-state")
            .setIndices()
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(getSnapshot("test-repo", "test-snap-with-global-state").state(), equalTo(SnapshotState.SUCCESS));
        snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo").addSnapshots("test-snap-with-global-state").get();
        assertThat(snapshotsStatusResponse.getSnapshots().size(), equalTo(1));
        snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
        assertThat(snapshotStatus.includeGlobalState(), equalTo(true));

        if (testTemplate) {
            logger.info("-->  delete test template");
            cluster().wipeTemplates("test-template");
            GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
            assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");
        }

        if (testPipeline) {
            logger.info("-->  delete test pipeline");
            assertAcked(clusterAdmin().deletePipeline(new DeletePipelineRequest("barbaz")).get());
        }

        if (testScript) {
            logger.info("-->  delete test script");
            assertAcked(clusterAdmin().prepareDeleteStoredScript("foobar").get());
        }

        logger.info("--> try restoring cluster state from snapshot without global state");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        logger.info("--> check that template wasn't restored");
        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> restore cluster state");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-with-global-state")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        if (testTemplate) {
            logger.info("--> check that template is restored");
            getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
            assertIndexTemplateExists(getIndexTemplatesResponse, "test-template");
        }

        if (testPipeline) {
            logger.info("--> check that pipeline is restored");
            GetPipelineResponse getPipelineResponse = clusterAdmin().prepareGetPipeline("barbaz").get();
            assertTrue(getPipelineResponse.isFound());
        }

        if (testScript) {
            logger.info("--> check that script is restored");
            GetStoredScriptResponse getStoredScriptResponse = clusterAdmin().prepareGetStoredScript("foobar").get();
            assertNotNull(getStoredScriptResponse.getSource());
        }

        createIndexWithRandomDocs("test-idx", 100);

        logger.info("--> snapshot without global state but with indices");
        createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-no-global-state-with-index")
            .setIndices("test-idx")
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(getSnapshot("test-repo", "test-snap-no-global-state-with-index").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("-->  delete global state and index ");
        cluster().wipeIndices("test-idx");
        if (testTemplate) {
            cluster().wipeTemplates("test-template");
        }
        if (testPipeline) {
            assertAcked(clusterAdmin().deletePipeline(new DeletePipelineRequest("barbaz")).get());
        }

        if (testScript) {
            assertAcked(clusterAdmin().prepareDeleteStoredScript("foobar").get());
        }

        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> try restoring index and cluster state from snapshot without global state");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state-with-index")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        logger.info("--> check that global state wasn't restored but index was");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");
        assertFalse(clusterAdmin().prepareGetPipeline("barbaz").get().isFound());
        assertNull(clusterAdmin().prepareGetStoredScript("foobar").get().getSource());
        assertDocCount("test-idx", 100L);
    }
}
