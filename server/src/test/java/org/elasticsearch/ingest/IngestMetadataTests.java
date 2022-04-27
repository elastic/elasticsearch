/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class IngestMetadataTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        PipelineConfiguration pipeline = new PipelineConfiguration("1", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);
        PipelineConfiguration pipeline2 = new PipelineConfiguration("2", new BytesArray("""
            {"processors": [{"set" : {"field": "_field1", "value": "_value1"}}]}"""), XContentType.JSON);
        Map<String, PipelineConfiguration> map = new HashMap<>();
        map.put(pipeline.getId(), pipeline);
        map.put(pipeline2.getId(), pipeline2);
        IngestMetadata ingestMetadata = new IngestMetadata(map);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        builder.prettyPrint();
        builder.startObject();
        ingestMetadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentBuilder shuffled = shuffleXContent(builder);
        try (XContentParser parser = createParser(shuffled)) {
            Metadata.Custom custom = IngestMetadata.fromXContent(parser);
            assertTrue(custom instanceof IngestMetadata);
            IngestMetadata m = (IngestMetadata) custom;
            assertEquals(2, m.getPipelines().size());
            assertEquals("1", m.getPipelines().get("1").getId());
            assertEquals("2", m.getPipelines().get("2").getId());
            assertEquals(pipeline.getConfigAsMap(), m.getPipelines().get("1").getConfigAsMap());
            assertEquals(pipeline2.getConfigAsMap(), m.getPipelines().get("2").getConfigAsMap());
        }
    }

    public void testDiff() throws Exception {
        BytesReference pipelineConfig = new BytesArray("{}");

        Map<String, PipelineConfiguration> pipelines = new HashMap<>();
        pipelines.put("1", new PipelineConfiguration("1", pipelineConfig, XContentType.JSON));
        pipelines.put("2", new PipelineConfiguration("2", pipelineConfig, XContentType.JSON));
        IngestMetadata ingestMetadata1 = new IngestMetadata(pipelines);

        pipelines = new HashMap<>();
        pipelines.put("1", new PipelineConfiguration("1", pipelineConfig, XContentType.JSON));
        pipelines.put("3", new PipelineConfiguration("3", pipelineConfig, XContentType.JSON));
        pipelines.put("4", new PipelineConfiguration("4", pipelineConfig, XContentType.JSON));
        IngestMetadata ingestMetadata2 = new IngestMetadata(pipelines);

        IngestMetadata.IngestMetadataDiff diff = (IngestMetadata.IngestMetadataDiff) ingestMetadata2.diff(ingestMetadata1);
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getDeletes().size(), equalTo(1));
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getDeletes().get(0), equalTo("2"));
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getUpserts().size(), equalTo(2));
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getUpserts().containsKey("3"), is(true));
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getUpserts().containsKey("4"), is(true));

        IngestMetadata endResult = (IngestMetadata) diff.apply(ingestMetadata2);
        assertThat(endResult, not(equalTo(ingestMetadata1)));
        assertThat(endResult.getPipelines().size(), equalTo(3));
        assertThat(endResult.getPipelines().get("1"), equalTo(new PipelineConfiguration("1", pipelineConfig, XContentType.JSON)));
        assertThat(endResult.getPipelines().get("3"), equalTo(new PipelineConfiguration("3", pipelineConfig, XContentType.JSON)));
        assertThat(endResult.getPipelines().get("4"), equalTo(new PipelineConfiguration("4", pipelineConfig, XContentType.JSON)));

        pipelines = new HashMap<>();
        pipelines.put("1", new PipelineConfiguration("1", new BytesArray("{}"), XContentType.JSON));
        pipelines.put("2", new PipelineConfiguration("2", new BytesArray("{}"), XContentType.JSON));
        IngestMetadata ingestMetadata3 = new IngestMetadata(pipelines);

        diff = (IngestMetadata.IngestMetadataDiff) ingestMetadata3.diff(ingestMetadata1);
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getDeletes().size(), equalTo(0));
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getUpserts().size(), equalTo(0));

        endResult = (IngestMetadata) diff.apply(ingestMetadata3);
        assertThat(endResult, equalTo(ingestMetadata1));
        assertThat(endResult.getPipelines().size(), equalTo(2));
        assertThat(endResult.getPipelines().get("1"), equalTo(new PipelineConfiguration("1", pipelineConfig, XContentType.JSON)));
        assertThat(endResult.getPipelines().get("2"), equalTo(new PipelineConfiguration("2", pipelineConfig, XContentType.JSON)));

        pipelines = new HashMap<>();
        pipelines.put("1", new PipelineConfiguration("1", new BytesArray("{}"), XContentType.JSON));
        pipelines.put("2", new PipelineConfiguration("2", new BytesArray("{\"key\" : \"value\"}"), XContentType.JSON));
        IngestMetadata ingestMetadata4 = new IngestMetadata(pipelines);

        diff = (IngestMetadata.IngestMetadataDiff) ingestMetadata4.diff(ingestMetadata1);
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getDiffs().size(), equalTo(1));
        assertThat(((DiffableUtils.MapDiff) diff.pipelines).getDiffs().containsKey("2"), is(true));

        endResult = (IngestMetadata) diff.apply(ingestMetadata4);
        assertThat(endResult, not(equalTo(ingestMetadata1)));
        assertThat(endResult.getPipelines().size(), equalTo(2));
        assertThat(endResult.getPipelines().get("1"), equalTo(new PipelineConfiguration("1", pipelineConfig, XContentType.JSON)));
        assertThat(
            endResult.getPipelines().get("2"),
            equalTo(new PipelineConfiguration("2", new BytesArray("{\"key\" : \"value\"}"), XContentType.JSON))
        );
    }
}
