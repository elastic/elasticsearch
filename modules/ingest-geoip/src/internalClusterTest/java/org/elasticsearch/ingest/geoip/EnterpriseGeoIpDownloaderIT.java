/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import fixture.geoip.EnterpriseGeoIpHttpFixture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.ingest.EnterpriseGeoIpTask;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration;
import org.elasticsearch.ingest.geoip.direct.DeleteDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.PutDatabaseConfigurationAction;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.EnterpriseGeoIpDownloaderTaskExecutor.IPINFO_TOKEN_SETTING;
import static org.elasticsearch.ingest.geoip.EnterpriseGeoIpDownloaderTaskExecutor.MAXMIND_LICENSE_KEY_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class EnterpriseGeoIpDownloaderIT extends ESIntegTestCase {

    private static final String MAXMIND_DATABASE_TYPE = "GeoIP2-City";
    private static final String IPINFO_DATABASE_TYPE = "asn";
    private static final String MAXMIND_CONFIGURATION = "test-1";
    private static final String IPINFO_CONFIGURATION = "test-2";

    @ClassRule
    public static final EnterpriseGeoIpHttpFixture fixture = new EnterpriseGeoIpHttpFixture(
        List.of(MAXMIND_DATABASE_TYPE),
        List.of(IPINFO_DATABASE_TYPE)
    );

    protected String getEndpoint() {
        return fixture.getAddress();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(MAXMIND_LICENSE_KEY_SETTING.getKey(), "license_key");
        secureSettings.setString(IPINFO_TOKEN_SETTING.getKey(), "token");
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
    @TestLogging(
        reason = "understanding why ipinfo asn database sometimes is not loaded",
        value = "org.elasticsearch.ingest.geoip.DatabaseNodeService:TRACE"
    )
    public void testEnterpriseDownloaderTask() throws Exception {
        /*
         * This test starts the enterprise geoip downloader task, and creates a database configuration. Then it creates an ingest
         * pipeline that references that database, and ingests a single document using that pipeline. It then asserts that the document
         * was updated with information from the database.
         * Note that the "enterprise database" is actually just a geolite database being loaded by the GeoIpHttpFixture.
         */
        EnterpriseGeoIpDownloader.DEFAULT_MAXMIND_ENDPOINT = getEndpoint();
        EnterpriseGeoIpDownloader.DEFAULT_IPINFO_ENDPOINT = getEndpoint();
        final String indexName = "enterprise_geoip_test_index";
        final String geoipPipelineName = "enterprise_geoip_pipeline";
        final String iplocationPipelineName = "enterprise_iplocation_pipeline";
        final String sourceField = "ip";
        final String targetField = "ip-result";

        startEnterpriseGeoIpDownloaderTask();
        configureMaxmindDatabase(MAXMIND_DATABASE_TYPE);
        configureIpinfoDatabase(IPINFO_DATABASE_TYPE);
        waitAround();
        createPipeline(geoipPipelineName, "geoip", MAXMIND_DATABASE_TYPE, sourceField, targetField);
        createPipeline(iplocationPipelineName, "ip_location", IPINFO_DATABASE_TYPE, sourceField, targetField);

        /*
         * We know that the databases index has been populated (because we waited around, :wink:), but we don't know for sure that
         * the databases have been pulled down and made available on all nodes. So we run these ingest-and-check steps in assertBusy blocks.
         */
        assertBusy(() -> {
            logger.info("Ingesting a test document");
            String documentId = ingestDocument(indexName, geoipPipelineName, sourceField, "89.160.20.128");
            GetResponse getResponse = client().get(new GetRequest(indexName, documentId)).actionGet();
            Map<String, Object> returnedSource = getResponse.getSource();
            assertNotNull(returnedSource);
            Object targetFieldValue = returnedSource.get(targetField);
            assertNotNull(targetFieldValue);
            assertThat(((Map<String, Object>) targetFieldValue).get("city_name"), equalTo("Linköping"));
        });
        assertBusy(() -> {
            logger.info("Ingesting another test document");
            String documentId = ingestDocument(indexName, iplocationPipelineName, sourceField, "103.134.48.0");
            GetResponse getResponse = client().get(new GetRequest(indexName, documentId)).actionGet();
            Map<String, Object> returnedSource = getResponse.getSource();
            assertNotNull(returnedSource);
            Object targetFieldValue = returnedSource.get(targetField);
            assertNotNull(targetFieldValue);
            assertThat(((Map<String, Object>) targetFieldValue).get("organization_name"), equalTo("PT Nevigate Telekomunikasi Indonesia"));
        });
    }

    @After
    public void cleanup() throws InterruptedException {
        /*
         * This method cleans up the database configurations that the test created. This allows the test to be run repeatedly.
         */
        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<AcknowledgedResponse> listener = new LatchedActionListener<>(ActionListener.noop(), latch);
        SubscribableListener.<AcknowledgedResponse>newForked(l -> deleteDatabaseConfiguration(MAXMIND_CONFIGURATION, l))
            .<AcknowledgedResponse>andThen(l -> deleteDatabaseConfiguration(IPINFO_CONFIGURATION, l))
            .addListener(listener);
        latch.await(10, TimeUnit.SECONDS);
    }

    private void deleteDatabaseConfiguration(String configurationName, ActionListener<AcknowledgedResponse> listener) {
        admin().cluster()
            .execute(
                DeleteDatabaseConfigurationAction.INSTANCE,
                new DeleteDatabaseConfigurationAction.Request(TimeValue.MAX_VALUE, TimeValue.timeValueSeconds(10), configurationName),
                listener
            );
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

    private void configureMaxmindDatabase(String databaseType) {
        admin().cluster()
            .execute(
                PutDatabaseConfigurationAction.INSTANCE,
                new PutDatabaseConfigurationAction.Request(
                    TimeValue.MAX_VALUE,
                    TimeValue.MAX_VALUE,
                    new DatabaseConfiguration(MAXMIND_CONFIGURATION, databaseType, new DatabaseConfiguration.Maxmind("test_account"))
                )
            )
            .actionGet();
    }

    private void configureIpinfoDatabase(String databaseType) {
        admin().cluster()
            .execute(
                PutDatabaseConfigurationAction.INSTANCE,
                new PutDatabaseConfigurationAction.Request(
                    TimeValue.MAX_VALUE,
                    TimeValue.MAX_VALUE,
                    new DatabaseConfiguration(IPINFO_CONFIGURATION, databaseType, new DatabaseConfiguration.Ipinfo())
                )
            )
            .actionGet();
    }

    private void waitAround() throws Exception {
        ensureGreen(GeoIpDownloader.DATABASES_INDEX);
        assertBusy(() -> {
            SearchResponse searchResponse = client().search(new SearchRequest(GeoIpDownloader.DATABASES_INDEX)).actionGet();
            try {
                assertThat(searchResponse.getHits().getHits().length, equalTo(2));
            } finally {
                searchResponse.decRef();
            }
        });
    }

    private void createPipeline(String pipelineName, String processorType, String databaseType, String sourceField, String targetField)
        throws IOException {
        putJsonPipeline(pipelineName, (builder, params) -> {
            builder.field("description", "test");
            builder.startArray("processors");
            {
                builder.startObject();
                {
                    builder.startObject(processorType);
                    {
                        builder.field("field", sourceField);
                        builder.field("target_field", targetField);
                        builder.field("database_file", databaseType + ".mmdb");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder.endArray();
        });
    }

    private String ingestDocument(String indexName, String pipelineName, String sourceField, String value) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(indexName).source(Strings.format("""
            { "%s": "%s"}
            """, sourceField, value), XContentType.JSON).setPipeline(pipelineName));
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        BulkItemResponse[] bulkItemResponses = response.getItems();
        assertThat(bulkItemResponses.length, equalTo(1));
        assertThat(bulkItemResponses[0].status(), equalTo(RestStatus.CREATED));
        return bulkItemResponses[0].getId();
    }
}
