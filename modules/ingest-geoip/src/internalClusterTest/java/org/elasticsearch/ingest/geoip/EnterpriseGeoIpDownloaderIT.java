/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import fixture.geoip.EnterpriseGeoIpHttpFixture;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.EnterpriseGeoIpTask;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration;
import org.elasticsearch.ingest.geoip.direct.PutDatabaseConfigurationAction;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.EnterpriseGeoIpDownloaderTaskExecutor.MAXMIND_LICENSE_KEY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class EnterpriseGeoIpDownloaderIT extends ESIntegTestCase {

    private static final String DATABASE_TYPE = "GeoIP2-City";

    @ClassRule
    public static final EnterpriseGeoIpHttpFixture fixture = new EnterpriseGeoIpHttpFixture(DATABASE_TYPE);

    protected String getEndpoint() {
        return fixture.getAddress();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(MAXMIND_LICENSE_KEY_SETTING.getKey(), "license_key");
        Settings.Builder builder = Settings.builder();
        builder.setSecureSettings(secureSettings)
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true);
        // note: this is using the enterprise fixture for the regular downloader, too, as
        // a slightly hacky way of making the regular downloader not actually download any files
        builder.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getEndpoint());
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // the reindex plugin is (somewhat surprisingly) necessary in order to be able to delete-by-query,
        // which modules/ingest-geoip does to delete old chunks
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), IngestGeoIpPlugin.class, ReindexPlugin.class);
    }

    @SuppressWarnings("unchecked")
    public void testEnterpriseDownloaderTask() throws Exception {
        /*
         * This test starts the enterprise geoip downloader task, and creates a database configuration. Then it creates an ingest
         * pipeline that references that database, and ingests a single document using that pipeline. It then asserts that the document
         * was updated with information from the database.
         * Note that the "enterprise database" is actually just a geolite database being loaded by the GeoIpHttpFixture.
         */
        EnterpriseGeoIpDownloader.DEFAULT_MAXMIND_ENDPOINT = getEndpoint();
        final String pipelineName = "enterprise_geoip_pipeline";
        final String indexName = "enterprise_geoip_test_index";
        final String sourceField = "ip";
        final String targetField = "ip-city";

        startEnterpriseGeoIpDownloaderTask();
        configureDatabase(DATABASE_TYPE);
        createGeoIpPipeline(pipelineName, DATABASE_TYPE, sourceField, targetField);

        assertBusy(() -> {
            /*
             * We know that the .geoip_databases index has been populated, but we don't know for sure that the database has been pulled
             * down and made available on all nodes. So we run this ingest-and-check step in an assertBusy.
             */
            logger.info("Ingesting a test document");
            String documentId = ingestDocument(indexName, pipelineName, sourceField);
            GetResponse getResponse = client().get(new GetRequest(indexName, documentId)).actionGet();
            Map<String, Object> returnedSource = getResponse.getSource();
            assertNotNull(returnedSource);
            Object targetFieldValue = returnedSource.get(targetField);
            assertNotNull(targetFieldValue);
            assertThat(((Map<String, Object>) targetFieldValue).get("organization_name"), equalTo("Bredband2 AB"));
        });
    }

    private void startEnterpriseGeoIpDownloaderTask() {
        PersistentTasksService persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        persistentTasksService.sendStartRequest(
            ENTERPRISE_GEOIP_DOWNLOADER,
            ENTERPRISE_GEOIP_DOWNLOADER,
            new EnterpriseGeoIpTask.EnterpriseGeoIpTaskParams(),
            TimeValue.MAX_VALUE,
            ActionListener.wrap(r -> logger.debug("Started enterprise geoip downloader task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create enterprise geoip downloader task", e);
                }
            })
        );
    }

    private void configureDatabase(String databaseType) throws Exception {
        admin().cluster()
            .execute(
                PutDatabaseConfigurationAction.INSTANCE,
                new PutDatabaseConfigurationAction.Request(
                    TimeValue.MAX_VALUE,
                    TimeValue.MAX_VALUE,
                    new DatabaseConfiguration("test", databaseType, new DatabaseConfiguration.Maxmind("test_account"))
                )
            )
            .actionGet();
        ensureGreen(GeoIpDownloader.DATABASES_INDEX);
        assertBusy(() -> {
            SearchResponse searchResponse = client().search(new SearchRequest(GeoIpDownloader.DATABASES_INDEX)).actionGet();
            try {
                assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            } finally {
                searchResponse.decRef();
            }
        });
    }

    private void createGeoIpPipeline(String pipelineName, String databaseType, String sourceField, String targetField) throws IOException {
        final BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.field("description", "test");
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", sourceField);
                            builder.field("target_field", targetField);
                            builder.field("database_file", databaseType + ".mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        assertAcked(clusterAdmin().putPipeline(new PutPipelineRequest(pipelineName, bytes, XContentType.JSON)).actionGet());
    }

    private String ingestDocument(String indexName, String pipelineName, String sourceField) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(indexName).source("{\"" + sourceField + "\": \"89.160.20.128\"}", XContentType.JSON).setPipeline(pipelineName)
        );
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        BulkItemResponse[] bulkItemResponses = response.getItems();
        assertThat(bulkItemResponses.length, equalTo(1));
        assertThat(bulkItemResponses[0].status(), equalTo(RestStatus.CREATED));
        return bulkItemResponses[0].getId();
    }
}
