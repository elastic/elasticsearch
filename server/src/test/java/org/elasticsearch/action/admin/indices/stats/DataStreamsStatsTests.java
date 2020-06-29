/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.EnumSet;
import java.util.List;

public class DataStreamsStatsTests extends ESSingleNodeTestCase {

    public void testStatsNoDataStream() throws Exception {
        DataStreamsStatsResponse stats = client().execute(DataStreamStatsAction.INSTANCE, new DataStreamsStatsRequest()).get();
        assertEquals(0, stats.getStreams());
        assertEquals(0, stats.getBackingIndices());
        assertEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(0, stats.getDataStreamStats().length);
    }

    public void testStatsEmptyDataStream() throws Exception {
        try {
            Template idxTemplate = new Template(null,
                new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"data\":{\"type\":\"keyword\"}}}"), null);
            ComposableIndexTemplate template = new ComposableIndexTemplate(List.of("my-data-stream*"), idxTemplate, null, null, null, null,
                new ComposableIndexTemplate.DataStreamTemplate("@timestamp"));
            assertTrue(client().execute(PutComposableIndexTemplateAction.INSTANCE, new PutComposableIndexTemplateAction.Request("template")
                .indexTemplate(template)).actionGet().isAcknowledged());

            assertTrue(client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request("my-data-stream-1")).get()
                .isAcknowledged());

            DataStreamsStatsResponse stats = client().execute(DataStreamStatsAction.INSTANCE, new DataStreamsStatsRequest()).get();
            assertEquals(1, stats.getStreams());
            assertEquals(1, stats.getBackingIndices());
            assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
            assertEquals(1, stats.getDataStreamStats().length);
            assertEquals("my-data-stream-1", stats.getDataStreamStats()[0].getDataStreamName());
            assertEquals(0L, stats.getDataStreamStats()[0].getMaxTimestamp());
            assertNotEquals(0L, stats.getDataStreamStats()[0].getStoreSize().getBytes());
            assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreamStats()[0].getStoreSize().getBytes());
        } finally {
            assertTrue(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request("my-data-stream-1")).get()
                .isAcknowledged());
        }
    }
}
