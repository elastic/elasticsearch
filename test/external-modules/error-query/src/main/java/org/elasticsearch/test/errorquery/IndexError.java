/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.errorquery;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class IndexError implements Writeable, ToXContentFragment {
    enum ERROR_TYPE {
        WARNING,
        EXCEPTION,
        NONE
    }

    private final String indexName;
    private final int[] shardIds;
    private final ERROR_TYPE errorType;
    private final String message;
    private final int stallTimeSeconds;  // how long to wait before returning

    public IndexError(String indexName, int[] shardIds, ERROR_TYPE errorType, String message, int stallTime) {
        this.indexName = indexName;
        this.shardIds = shardIds;
        this.errorType = errorType;
        this.message = message;
        this.stallTimeSeconds = stallTime;
    }

    public IndexError(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.shardIds = in.readBoolean() ? in.readIntArray() : null;
        this.errorType = in.readEnum(ERROR_TYPE.class);
        this.message = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_500_051)) {
            this.stallTimeSeconds = in.readVInt();
        } else {
            this.stallTimeSeconds = 0;
        }
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_500_051)) {
            out.writeVInt(stallTimeSeconds);
        }
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

    public int getStallTimeSeconds() {
        return stallTimeSeconds;
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<IndexError, String> PARSER = new ConstructingObjectParser<>(
        "index_error",
        false,
        (args, name) -> {
            List<Integer> lst = (List<Integer>) args[1];
            int[] shardIds = lst == null ? null : lst.stream().mapToInt(i -> i).toArray();
            String message = args[3] == null ? "" : (String) args[3];
            int stallTime = args[4] == null ? 0 : (int) args[4];
            return new IndexError(
                (String) args[0],
                shardIds,
                ERROR_TYPE.valueOf(((String) args[2]).toUpperCase(Locale.ROOT)),
                message,
                stallTime
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareIntArray(optionalConstructorArg(), new ParseField("shard_ids"));
        PARSER.declareString(constructorArg(), new ParseField("error_type"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("message"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("stall_time_seconds"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", indexName);
        if (shardIds != null) {
            builder.field("shard_ids", shardIds);
        }
        builder.field("error_type", errorType.toString());
        builder.field("message", message);
        builder.field("stall_time_seconds", stallTimeSeconds);
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
            && message.equals(that.message)
            && stallTimeSeconds == stallTimeSeconds;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indexName, errorType, message, stallTimeSeconds);
        result = 31 * result + Arrays.hashCode(shardIds);
        return result;
    }

    @Override
    public String toString() {
        return "IndexError{"
            + "indexName='"
            + indexName
            + '\''
            + ", shardIds="
            + Arrays.toString(shardIds)
            + ", errorType="
            + errorType
            + ", message='"
            + message
            + '\''
            + ", stallTimeSeconds="
            + stallTimeSeconds
            + '}';
    }
}
