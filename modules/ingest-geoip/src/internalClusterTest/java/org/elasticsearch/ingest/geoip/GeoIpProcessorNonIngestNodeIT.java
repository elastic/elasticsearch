/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.test.NodeRoles.nonIngestNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class GeoIpProcessorNonIngestNodeIT extends AbstractGeoIpIT {

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
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        assertAcked(client().admin().cluster().putPipeline(new PutPipelineRequest("geoip", bytes, XContentType.JSON)).actionGet());
        // the geo-IP databases should not be loaded on any nodes as they are all non-ingest nodes
        Arrays.stream(internalCluster().getNodeNames()).forEach(node -> assertDatabaseLoadStatus(node, false));

        // start an ingest node
        final String ingestNode = internalCluster().startNode(NodeRoles.ingestNode());
        internalCluster().getInstance(IngestService.class, ingestNode);
        // the geo-IP database should not be loaded yet as we have no indexed any documents using a pipeline that has a geo-IP processor
        assertDatabaseLoadStatus(ingestNode, false);
        final IndexRequest indexRequest = new IndexRequest("index");
        indexRequest.setPipeline("geoip");
        indexRequest.source(Collections.singletonMap("ip", "1.1.1.1"));
        final IndexResponse indexResponse = client(ingestNode).index(indexRequest).actionGet();
        assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
        // now the geo-IP database should be loaded on the ingest node
        assertDatabaseLoadStatus(ingestNode, true);
        // the geo-IP database should still not be loaded on the non-ingest nodes
        Arrays.stream(internalCluster().getNodeNames())
            .filter(node -> node.equals(ingestNode) == false)
            .forEach(node -> assertDatabaseLoadStatus(node, false));
    }

    private void assertDatabaseLoadStatus(final String node, final boolean loaded) {
        final IngestService ingestService = internalCluster().getInstance(IngestService.class, node);
        final GeoIpProcessor.Factory factory = (GeoIpProcessor.Factory) ingestService.getProcessorFactories().get("geoip");
        for (final DatabaseReaderLazyLoader loader : factory.getAllDatabases()) {
            if (loaded) {
                assertNotNull(loader.databaseReader.get());
            } else {
                assertNull(loader.databaseReader.get());
            }
        }
    }

}
