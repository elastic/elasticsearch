/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, maxNumDataNodes = 1)
public class GeoIpDownloaderStatsIT extends AbstractGeoIpIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, IngestGeoIpPlugin.class, GeoIpProcessorNonIngestNodeIT.IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (getEndpoint() != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getEndpoint());
        }
        return settings.build();
    }

    @After
    public void disableDownloader() {
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), (String) null));
    }

    public void testStats() throws Exception {
        /*
         * Testing without the geoip endpoint fixture falls back to https://storage.googleapis.com/, which can cause this test to run too
         * slowly to pass.
         */
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        GeoIpDownloaderStatsAction.Request req = new GeoIpDownloaderStatsAction.Request();
        GeoIpDownloaderStatsAction.Response response = client().execute(GeoIpDownloaderStatsAction.INSTANCE, req).actionGet();
        XContentTestUtils.JsonMapView jsonMapView = new XContentTestUtils.JsonMapView(convertToMap(response));
        assertThat(jsonMapView.get("stats.successful_downloads"), equalTo(0));
        assertThat(jsonMapView.get("stats.failed_downloads"), equalTo(0));
        assertThat(jsonMapView.get("stats.skipped_updates"), equalTo(0));
        assertThat(jsonMapView.get("stats.databases_count"), equalTo(0));
        assertThat(jsonMapView.get("stats.total_download_time"), equalTo(0));
        assertEquals(0, jsonMapView.<Map<String, Object>>get("nodes").size());
        putPipeline();
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));

        assertBusy(() -> {
            GeoIpDownloaderStatsAction.Response res = client().execute(GeoIpDownloaderStatsAction.INSTANCE, req).actionGet();
            XContentTestUtils.JsonMapView view = new XContentTestUtils.JsonMapView(convertToMap(res));
            assertThat(view.get("stats.successful_downloads"), equalTo(3));
            assertThat(view.get("stats.failed_downloads"), equalTo(0));
            assertThat(view.get("stats.skipped_updates"), equalTo(0));
            assertThat(view.get("stats.databases_count"), equalTo(3));
            assertThat(view.get("stats.total_download_time"), greaterThan(0));
            Map<String, Map<String, List<Map<String, Object>>>> nodes = view.get("nodes");
            assertThat(nodes.values(), hasSize(greaterThan(0)));
            for (Map<String, List<Map<String, Object>>> value : nodes.values()) {
                assertThat(value, hasKey("databases"));
                assertThat(
                    value.get("databases").stream().map(m -> m.get("name")).collect(Collectors.toSet()),
                    containsInAnyOrder("GeoLite2-City.mmdb", "GeoLite2-ASN.mmdb", "GeoLite2-Country.mmdb")
                );
            }
        });
    }

    private void putPipeline() throws IOException {
        BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
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
        assertAcked(clusterAdmin().preparePutPipeline("_id", bytes, XContentType.JSON).get());
    }

    public static Map<String, Object> convertToMap(ToXContent part) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        part.toXContent(builder, EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
