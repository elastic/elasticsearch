/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class DefaultShardOperationFailedException extends ShardOperationFailedException implements Writeable {

    private static final String INDEX = "index";
    private static final String SHARD_ID = "shard";
    private static final String REASON = "reason";

    public static final ConstructingObjectParser<DefaultShardOperationFailedException, Void> PARSER = new ConstructingObjectParser<>(
        "failures",
        true,
        arg -> new DefaultShardOperationFailedException((String) arg[0], (int) arg[1], (Throwable) arg[2])
    );

    protected static <T extends DefaultShardOperationFailedException> void declareFields(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareString(constructorArg(), new ParseField(INDEX));
        objectParser.declareInt(constructorArg(), new ParseField(SHARD_ID));
        objectParser.declareObject(constructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), new ParseField(REASON));
    }

    static {
        declareFields(PARSER);
    }

    protected DefaultShardOperationFailedException() {}

    protected DefaultShardOperationFailedException(StreamInput in) throws IOException {
        readFrom(in, this);
    }

    public DefaultShardOperationFailedException(ElasticsearchException e) {
        super(
            e.getIndex() == null ? null : e.getIndex().getName(),
            e.getShardId() == null ? -1 : e.getShardId().getId(),
            ExceptionsHelper.stackTrace(e),
            e.status(),
            e
        );
    }

    public DefaultShardOperationFailedException(String index, int shardId, Throwable cause) {
        super(index, shardId, ExceptionsHelper.stackTrace(cause), ExceptionsHelper.status(cause), cause);
    }

    public static DefaultShardOperationFailedException readShardOperationFailed(StreamInput in) throws IOException {
        return new DefaultShardOperationFailedException(in);
    }

    public static void readFrom(StreamInput in, DefaultShardOperationFailedException f) throws IOException {
        f.index = in.readOptionalString();
        f.shardId = in.readVInt();
        f.cause = in.readException();
        f.status = RestStatus.readFrom(in);
        f.reason = ExceptionsHelper.stackTrace(f.cause);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
        out.writeVInt(shardId);
        out.writeException(cause);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String toString() {
        return "[" + index + "][" + shardId + "] failed, reason [" + reason() + "]";
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("shard", shardId());
        builder.field("index", index());
        builder.field("status", status.name());
        if (reason != null) {
            builder.startObject("reason");
            ElasticsearchException.generateThrowableXContent(builder, params, cause);
            builder.endObject();
        }
        return builder;
    }

    public static DefaultShardOperationFailedException fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
