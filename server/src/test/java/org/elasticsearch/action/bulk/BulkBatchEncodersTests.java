/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.io.stream.MockBytesRefRecycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

public class BulkBatchEncodersTests extends ESTestCase {

    /**
     * A malformed string the encoder staged without reading is found when the row is committed. Like a document the encoder
     * fails to parse, it abandons the batch rather than failing the item, which is then routed with its inline source.
     */
    public void testMalformedStringFoundOnCommitAbandonsTheBatch() {
        assumeTrue("strings are staged unread only on the simdjson path", EscfEncoder.isSimdEnabled());
        IndexMetadata index = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, "index-uuid")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(index, false).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(index);
        try (MockBytesRefRecycler recycler = new MockBytesRefRecycler(); BulkBatchEncoders encoders = new BulkBatchEncoders(recycler)) {
            assertEquals(0, encoders.tryEncodeAndRoute(request("{\"s\":\"ok\"}"), index.getIndex(), routing, project));
            assertEquals(
                BulkBatchEncoders.NOT_BATCHABLE,
                encoders.tryEncodeAndRoute(request("{\"s\":\"bad\\qescape\"}"), index.getIndex(), routing, project)
            );
            assertTrue(encoders.disabled());
        }
    }

    private static IndexRequest request(String source) {
        return new IndexRequest("index").id(randomAlphaOfLength(8)).source(source, XContentType.JSON);
    }
}
