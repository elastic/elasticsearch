/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to reshard an index.
 */
public class ReshardIndexRequest extends MasterNodeRequest<ReshardIndexRequest> implements IndicesRequest {

    static final ConstructingObjectParser<ReshardIndexRequest, Void> PARSER = new ConstructingObjectParser<>(
        "reshard_request",
        args -> new ReshardIndexRequest((String) args[0], (Integer) args[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("index"));
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("split_shard_count"));
    }

    private final String index;
    private final int newShardCount;

    public ReshardIndexRequest(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        newShardCount = in.readInt();
    }

    public ReshardIndexRequest(String index) {
        this(index, -1);
    }

    public ReshardIndexRequest(String index, int newShardCount) {
        super(INFINITE_MASTER_NODE_TIMEOUT);
        this.index = index;
        this.newShardCount = newShardCount;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(new String[] { index })) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    /**
     * Name of index we intend to autoshard.
     */
    public String index() {
        return index;
    }

    public int newShardCount() {
        return newShardCount;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeInt(newShardCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ReshardIndexRequest that = (ReshardIndexRequest) obj;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    public static ReshardIndexRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
