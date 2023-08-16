/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.index.IndexResponse;
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

public class SimulateIndexResponse extends IndexResponse {
    private final BytesReference source;
    private final XContentType sourceXContentType;
    private final List<String> pipelines;

    public SimulateIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.source = in.readBytesReference();
        this.sourceXContentType = XContentType.valueOf(in.readString());
        this.pipelines = in.readList(StreamInput::readString);
    }

    public SimulateIndexResponse(String index, BytesReference source, XContentType sourceXContentType, List<String> pipelines) {
        super(new ShardId(index, "", 0), "", 0, 0, 0, true);
        this.source = source;
        this.sourceXContentType = sourceXContentType;
        this.pipelines = pipelines;
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("_index", getShardId().getIndexName());
        builder.field("_source", XContentHelper.convertToMap(source, false, sourceXContentType).v2());
        builder.array("pipelines", pipelines.toArray());
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
        out.writeCollection(pipelines, StreamOutput::writeString);
    }

    // @Override
    // public String toString() {
    // StringBuilder builder = new StringBuilder();
    // builder.append("IndexResponse[");
    // builder.append("index=").append(getIndex());
    // builder.append(",pipelines=").append(Strings.toString(getPipelines()));
    // return builder.append("]").toString();
    // }
    //
    // private List<String> getPipelines() {
    // return pipelines;
    // }

}
