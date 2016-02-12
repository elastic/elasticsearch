/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IngestMetadataTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        PipelineConfiguration pipeline = new PipelineConfiguration(
            "1",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}")
        );
        PipelineConfiguration pipeline2 = new PipelineConfiguration(
            "2",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field1\", \"value\": \"_value1\"}}]}")
        );
        Map<String, PipelineConfiguration> map = new HashMap<>();
        map.put(pipeline.getId(), pipeline);
        map.put(pipeline2.getId(), pipeline2);
        IngestMetadata ingestMetadata = new IngestMetadata(map);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.prettyPrint();
        builder.startObject();
        ingestMetadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String string = builder.string();
        final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(string);
        MetaData.Custom custom = ingestMetadata.fromXContent(parser);
        assertTrue(custom instanceof IngestMetadata);
        IngestMetadata m = (IngestMetadata) custom;
        assertEquals(2, m.getPipelines().size());
        assertEquals("1", m.getPipelines().get("1").getId());
        assertEquals("2", m.getPipelines().get("2").getId());
        assertEquals(pipeline.getConfigAsMap(), m.getPipelines().get("1").getConfigAsMap());
        assertEquals(pipeline2.getConfigAsMap(), m.getPipelines().get("2").getConfigAsMap());
    }
}
