/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * This is an IndexResponse that is specifically for simulate requests. Unlike typical IndexResponses, we need to include the original
 * source in a SimulateIndexResponse, and don't need most other fields. This has to extend IndexResponse though so that it can be used by
 * BulkItemResponse in IngestService.
 */
public class SimulateIndexResponse extends IndexResponse {
    private final BytesReference source;
    private final XContentType sourceXContentType;

    @SuppressWarnings("this-escape")
    public SimulateIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.source = in.readBytesReference();
        this.sourceXContentType = XContentType.valueOf(in.readString());
        setShardInfo(new ReplicationResponse.ShardInfo(0, 0));
    }

    @SuppressWarnings("this-escape")
    public SimulateIndexResponse(
        String id,
        String index,
        long version,
        BytesReference source,
        XContentType sourceXContentType,
        List<String> pipelines
    ) {
        // We don't actually care about most of the IndexResponse fields:
        super(new ShardId(index, "", 0), id == null ? "<n/a>" : id, 0, 0, version, true, pipelines);
        this.source = source;
        this.sourceXContentType = sourceXContentType;
        setShardInfo(new ReplicationResponse.ShardInfo(0, 0));
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("_id", getId());
        builder.field("_index", getShardId().getIndexName());
        builder.field("_version", getVersion());
        builder.field("_source", XContentHelper.convertToMap(source, false, sourceXContentType).v2());
        assert executedPipelines != null : "executedPipelines is null when it shouldn't be - we always list pipelines in simulate mode";
        builder.array("executed_pipelines", executedPipelines.toArray());
        return builder;
    }

    @Override
    public RestStatus status() {
        return RestStatus.CREATED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);
        out.writeString(sourceXContentType.name());
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SimulateIndexResponse[");
        builder.append("index=").append(getIndex());
        try {
            builder.append(",source=").append(XContentHelper.convertToJson(source, false, sourceXContentType));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        builder.append(",pipelines=[").append(String.join(", ", executedPipelines));
        return builder.append("]]").toString();
    }
}
