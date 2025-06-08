/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package geoip;

import fixture.geoip.GeoIpHttpFixture;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.geoip.GeoIpDownloader;
import org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor;
import org.elasticsearch.ingest.geoip.GeoIpTaskState;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.multiproject.TestOnlyMultiProjectPlugin;
import org.elasticsearch.multiproject.action.PutProjectAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import org.elasticsearch.core.Booleans;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class GeoIpDownloaderMultiProjectIT extends ESIntegTestCase {
    // default true
    private static final boolean useFixture = Booleans.parseBoolean(System.getProperty("geoip_use_service", "false")) == false;

    @ClassRule
    public static final GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            IngestGeoIpPlugin.class, TestOnlyMultiProjectPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal, final Settings otherSettings) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("test.multi_project.enabled", "true")
            .put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true);

        if (useFixture) {
            builder.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), fixture.getAddress());
        }

        return builder.build();
    }

    public void testGeoIp() throws Exception {
        ProjectId project1 = randomUniqueProjectId();
        createProject(project1);
        putGeoIpPipeline(project1, "_id_1");
        assertBusy(() -> {
            GeoIpTaskState state = (GeoIpTaskState) getTask(project1).getState();
            assertThat(state, notNullValue());
            assertThat(
                state.getDatabases().keySet(),
                containsInAnyOrder("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb")
            );
        }, 30, TimeUnit.SECONDS);
    }

    private PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> getTask(ProjectId projectId) {
        return PersistentTasksCustomMetadata.getTaskWithId(clusterService().state().projectState(projectId).metadata(),
            GeoIpDownloaderTaskExecutor.getTaskId(projectId, true));
    }

    private void createProject(ProjectId projectId) {
        PutProjectAction.Request putProjectRequest = new PutProjectAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            projectId
        );
        assertAcked(safeGet(client().execute(PutProjectAction.INSTANCE, putProjectRequest)));
    }

    private void putGeoIpPipeline(ProjectId projectId, String pipelineId) throws IOException {
        TestProjectResolvers.singleProject(projectId).executeOnProject(projectId, () -> {
            putJsonPipeline(pipelineId, ((builder, params) -> {
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
                            builder.field("download_database_on_pipeline_creation", true);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-country");
                            builder.field("database_file", "GeoLite2-Country.mmdb");
                            builder.field("download_database_on_pipeline_creation", false);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-asn");
                            builder.field("database_file", "GeoLite2-ASN.mmdb");
                            builder.field("download_database_on_pipeline_creation", false);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "MyCustomGeoLite2-City.mmdb");
                            builder.field("download_database_on_pipeline_creation", false);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                return builder.endArray();
            }));
        });

    }

//
//    private void putGeoIpPipeline(String projectId) throws IOException {
//        Request request = new Request("PUT", "/_index_template/fs-template");
//        request.setJsonEntity("""
//                  {
//                  "index_patterns": ["fs*"],
//                  "data_stream": {},
//                  "template": {
//                    "data_stream_options": {
//                      "failure_store": {
//                        "enabled": true
//                      }
//                    }
//                  }
//                }
//                """);
//
//        setRequestProjectId(request, projectId.toString());
//
//        IngestPipelineTestUtils.putJsonPipeline(client(), pipelineId, (builder, params) -> {
//            builder.startArray("processors");
//            {
//                builder.startObject();
//                {
//                    builder.startObject("geoip");
//                    {
//                        builder.field("field", "ip");
//                        builder.field("target_field", "ip-city");
//                        builder.field("database_file", "GeoLite2-City.mmdb");
//                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
//                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
//                        }
//                    }
//                    builder.endObject();
//                }
//                builder.endObject();
//                builder.startObject();
//                {
//                    builder.startObject("geoip");
//                    {
//                        builder.field("field", "ip");
//                        builder.field("target_field", "ip-country");
//                        builder.field("database_file", "GeoLite2-Country.mmdb");
//                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
//                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
//                        }
//                    }
//                    builder.endObject();
//                }
//                builder.endObject();
//                builder.startObject();
//                {
//                    builder.startObject("geoip");
//                    {
//                        builder.field("field", "ip");
//                        builder.field("target_field", "ip-asn");
//                        builder.field("database_file", "GeoLite2-ASN.mmdb");
//                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
//                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
//                        }
//                    }
//                    builder.endObject();
//                }
//                builder.endObject();
//                builder.startObject();
//                {
//                    builder.startObject("geoip");
//                    {
//                        builder.field("field", "ip");
//                        builder.field("target_field", "ip-city");
//                        builder.field("database_file", "MyCustomGeoLite2-City.mmdb");
//                        if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
//                            builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
//                        }
//                    }
//                    builder.endObject();
//                }
//                builder.endObject();
//            }
//            return builder.endArray();
//        });
//    }

}
