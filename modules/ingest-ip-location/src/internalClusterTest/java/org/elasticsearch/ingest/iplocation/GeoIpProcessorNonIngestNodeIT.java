/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.geoip.AbstractGeoIpIT;
import org.elasticsearch.ingest.geoip.DatabaseNodeService;
import org.elasticsearch.ingest.geoip.DatabaseReaderLazyLoader;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.NodeRoles;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.NodeRoles.nonIngestNode;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that geoip databases are lazily loaded on ingest nodes and not loaded on non-ingest nodes.
 * Moved from ip-location because it depends on the geoip processor type from {@link IngestIpLocationPlugin}.
 */
public class GeoIpProcessorNonIngestNodeIT extends AbstractGeoIpIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class, IngestIpLocationPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put(nonIngestNode()).build();
    }

    /**
     * This test shows that we do not load the geo-IP databases on non-ingest nodes, and only load on ingest nodes on first use.
     *
     * @throws IOException if an I/O exception occurs building the JSON
     */
    public void testLazyLoading() throws IOException {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/37342", Constants.WINDOWS);
        putJsonPipeline("geoip", (builder, params) -> {
            builder.field("description", "test");
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
                builder.startObject();
                {
                    builder.startObject("geoip");
                    {
                        builder.field("field", "ip");
                        builder.field("target_field", "ip-country");
                        builder.field("database_file", "GeoLite2-Country.mmdb");
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
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder.endArray();
        });
        Arrays.stream(internalCluster().getNodeNames()).forEach(node -> assertDatabaseLoadStatus(node, false));

        final String ingestNode = internalCluster().startNode(NodeRoles.ingestNode());
        internalCluster().getInstance(IngestService.class, ingestNode);
        assertDatabaseLoadStatus(ingestNode, false);
        final IndexRequest indexRequest = new IndexRequest("index");
        indexRequest.setPipeline("geoip");
        indexRequest.source(Map.of("ip", "1.1.1.1"));
        final DocWriteResponse indexResponse = client(ingestNode).index(indexRequest).actionGet();
        assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
        assertDatabaseLoadStatus(ingestNode, true);
        Arrays.stream(internalCluster().getNodeNames())
            .filter(node -> node.equals(ingestNode) == false)
            .forEach(node -> assertDatabaseLoadStatus(node, false));
    }

    private void assertDatabaseLoadStatus(final String node, final boolean loaded) {
        final DatabaseNodeService databaseNodeService = internalCluster().getInstance(DatabaseNodeService.class, node);
        for (final DatabaseReaderLazyLoader loader : databaseNodeService.getAllDatabases()) {
            if (loaded) {
                assertTrue("database should be loaded on node [" + node + "]", loader.isLoaded());
            } else {
                assertFalse("database should not be loaded on node [" + node + "]", loader.isLoaded());
            }
        }
    }
}
