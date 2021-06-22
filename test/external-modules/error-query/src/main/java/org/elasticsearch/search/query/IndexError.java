/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class IndexError implements Writeable, ToXContentFragment {
    enum ERROR_TYPE {
        WARNING,
        EXCEPTION
    }

    private final String indexName;
    private final int[] shardIds;
    private final ERROR_TYPE errorType;
    private final String message;

    public IndexError(String indexName, int[] shardIds, ERROR_TYPE errorType, String message) {
        this.indexName = indexName;
        this.shardIds = shardIds;
        this.errorType = errorType;
        this.message = message;
    }

    public IndexError(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.shardIds = in.readBoolean() ? in.readIntArray() : null;
        this.errorType = in.readEnum(ERROR_TYPE.class);
        this.message = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeBoolean(shardIds != null);
        if (shardIds != null) {
            out.writeIntArray(shardIds);
        }
        out.writeEnum(errorType);
        out.writeString(message);
    }

    public String getIndexName() {
        return indexName;
    }

    @Nullable
    public int[] getShardIds() {
        return shardIds;
    }

    public ERROR_TYPE getErrorType() {
        return errorType;
    }

    public String getMessage() {
        return message;
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<IndexError, String> PARSER = new ConstructingObjectParser<>(
        "index_error",
        false,
        (args, name) -> {
            List<Integer> lst = (List<Integer>) args[1];
            int[] shardIds = lst == null ? null : lst.stream().mapToInt(i -> i).toArray();
            return new IndexError(
                (String) args[0],
                shardIds,
                ERROR_TYPE.valueOf(((String) args[2]).toUpperCase(Locale.ROOT)),
                (String) args[3]
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareIntArray(optionalConstructorArg(), new ParseField("shard_ids"));
        PARSER.declareString(constructorArg(), new ParseField("error_type"));
        PARSER.declareString(constructorArg(), new ParseField("message"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", indexName);
        if (shardIds != null) {
            builder.field("shard_ids", shardIds);
        }
        builder.field("error_type", errorType.toString());
        builder.field("message", message);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexError that = (IndexError) o;
        return indexName.equals(that.indexName)
            && Arrays.equals(shardIds, that.shardIds)
            && errorType == that.errorType
            && message.equals(that.message);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indexName, errorType, message);
        result = 31 * result + Arrays.hashCode(shardIds);
        return result;
    }
}
