/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class TimeSeriesIndexIT extends AbstractRollingTestCase {

    private record DocumentHolder(String timestamp, String host, double gauge, long counter) {}

    private int lastIndexedDoc = 0;

    private static final String INDEX_NAME = "test";
    private static final Instant NOW = Instant.ofEpochMilli(System.currentTimeMillis());
    private static final List<DocumentHolder> DOCUMENTS = List.of(
        new DocumentHolder(NOW.plus(1, ChronoUnit.MINUTES).toString(), "host-a", 12.0, 3),
        new DocumentHolder(NOW.plus(2, ChronoUnit.MINUTES).toString(), "host-a", 13.0, 4),
        new DocumentHolder(NOW.plus(3, ChronoUnit.MINUTES).toString(), "host-a", 15.0, 5),
        new DocumentHolder(NOW.plus(4, ChronoUnit.MINUTES).toString(), "host-a", 18.0, 10),
        new DocumentHolder(NOW.plus(5, ChronoUnit.MINUTES).toString(), "host-a", 19.0, 12),
        new DocumentHolder(NOW.plus(6, ChronoUnit.MINUTES).toString(), "host-a", 22.0, 20),
        new DocumentHolder(NOW.plus(7, ChronoUnit.MINUTES).toString(), "host-a", 25.0, 30),
        new DocumentHolder(NOW.plus(8, ChronoUnit.MINUTES).toString(), "host-a", 26.0, 31)
    );

    public void testTimeSeriesIndexSettings() throws IOException {
        assumeTrue(
            "Fix for time series indices required settings introduced in 8.11",
            UPGRADE_FROM_VERSION.between(Version.V_8_7_0, Version.V_8_11_0)
        );
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createTimeSeriesIndex(INDEX_NAME);
            assertIndexExists(INDEX_NAME);
            indexDocuments(INDEX_NAME, lastIndexedDoc, lastIndexedDoc + 5);
            lastIndexedDoc = 5;
            assertDocCount(client(), INDEX_NAME, 5);
            assertSearch(INDEX_NAME);
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            assertIndexExists(INDEX_NAME);
            assertSearch(INDEX_NAME);
            if (lastIndexedDoc == 5) {
                assertDocCount(client(), INDEX_NAME, 5);
                indexDocuments(INDEX_NAME, lastIndexedDoc, lastIndexedDoc + 1);
                assertDocCount(client(), INDEX_NAME, 6);
                lastIndexedDoc = 6;
            } else if (lastIndexedDoc == 6) {
                assertDocCount(client(), INDEX_NAME, 6);
                indexDocuments(INDEX_NAME, lastIndexedDoc, lastIndexedDoc + 1);
                assertDocCount(client(), INDEX_NAME, 7);
                lastIndexedDoc = 7;
            }
            assertSearch(INDEX_NAME);
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            assertIndexExists(INDEX_NAME);
            assertDocCount(client(), INDEX_NAME, 7);
            assertSearch(INDEX_NAME);
            indexDocuments(INDEX_NAME, lastIndexedDoc, lastIndexedDoc + 1);
            assertDocCount(client(), INDEX_NAME, 8);
            assertSearch(INDEX_NAME);
        }
    }

    private static void assertIndexExists(final String indexName) throws IOException {
        assertOK(client().performRequest(new Request("GET", "/" + indexName)));
    }

    private static void createTimeSeriesIndex(final String indexName) throws IOException {
        final Request createIndexRequest = new Request("PUT", "/" + indexName);
        final XContentBuilder mappings = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("settings")
            .field("mode", "time_series")
            .field("routing_path", "host")
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("host")
            .field("type", "keyword")
            .field("time_series_dimension", "true")
            .endObject()
            .startObject("gauge")
            .field("type", "double")
            .field("time_series_metric", "gauge")
            .endObject()
            .startObject("counter")
            .field("type", "long")
            .field("time_series_metric", "counter")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndexRequest.setJsonEntity(Strings.toString(mappings));
        assertOK(client().performRequest(createIndexRequest));
    }

    private static void indexDocuments(final String indexName, int start, int end) throws IOException {
        for (int i = start; i < end; i++) {
            final DocumentHolder doc = DOCUMENTS.get(i);
            indexDocument(indexName, doc.timestamp, doc.host, doc.gauge, doc.counter);
        }
    }

    private static void indexDocument(final String indexName, final String timestamp, final String host, double gauge, long counter)
        throws IOException {
        final XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field("@timestamp", timestamp)
            .field("host", host)
            .field("gauge", gauge)
            .field("counter", counter)
            .endObject();
        final Request indexRequest = new Request("POST", "/" + indexName + "/_doc");
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity(Strings.toString(doc));
        assertOK(client().performRequest(indexRequest));
    }

    private static void assertSearch(final String indexName) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + indexName + "/_search")));
    }
}
