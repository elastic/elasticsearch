/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.EnterpriseGeoIpDownloaderTaskExecutor.MAXMIND_LICENSE_KEY_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class EnterpriseGeoIpDownloaderIT extends AbstractGeoIpIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(MAXMIND_LICENSE_KEY_SETTING.getKey(), "license_key");
        Settings.Builder builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        return builder.build();
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ReindexPlugin.class);
    }

    public void testEnterpriseDownloaderTask() throws Exception {
        /*
         * This test starts the enterprise geoip downloader task, and creates a database configuration. Then it creates an ingest
         * pipeline that references that database, and ingests a single document using that pipeline. It then asserts that the document
         * was updated with information from the datbase.
         * Note that the "enterprise database" is actually just a geolite database being loaded by the GeoIpHttpFixture.
         */
        if (getEndpoint() != null) {
            EnterpriseGeoIpDownloader.DEFAULT_MAXMIND_ENDPOINT = getEndpoint();
        }
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
        admin().cluster()
            .execute(
                PutDatabaseConfigurationAction.INSTANCE,
                new PutDatabaseConfigurationAction.Request(
                    TimeValue.MAX_VALUE,
                    TimeValue.MAX_VALUE,
                    new DatabaseConfiguration("test", "GeoIP2-City", new DatabaseConfiguration.Maxmind("test_account"))
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
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoIP2-City.mmdb");
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
        assertAcked(clusterAdmin().putPipeline(new PutPipelineRequest("geoip", bytes, XContentType.JSON)).actionGet());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("index1").id("1").source("{\"ip\": \"89.160.20.128\"}", XContentType.JSON).setPipeline("geoip"));
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(1));
        assertThat(response.getItems()[0].status(), equalTo(RestStatus.CREATED));

        GetResponse getResponse = client().get(new GetRequest("index1", response.getItems()[0].getId())).actionGet();
        Map<String, Object> returnedSource = getResponse.getSource();
        assertNotNull(returnedSource);
        assertNotNull(returnedSource.get("ip-city"));

    }
}
