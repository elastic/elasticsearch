/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class NodesRemovalPrevalidation implements ToXContentFragment, Writeable {

    private final Result overallResult;
    private final Map<String, Result> perNodeResult;

    public NodesRemovalPrevalidation(Result overallResult, Map<String, Result> perNodeResult) {
        this.overallResult = overallResult;
        this.perNodeResult = perNodeResult;
    }

    public NodesRemovalPrevalidation(final StreamInput in) throws IOException {
        this.overallResult = Result.readFrom(in);
        this.perNodeResult = in.readMap(StreamInput::readString, Result::readFrom);
    }

    public Result getOverallResult() {
        return overallResult;
    }

    public Map<String, Result> getPerNodeResult() {
        return perNodeResult;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("overall_result");
        overallResult.toXContent(builder, params);
        builder.endObject();
        builder.startObject("per_node_result");
        for (Map.Entry<String, Result> entry : perNodeResult.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        overallResult.writeTo(out);
        out.writeMap(perNodeResult, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public record Result(IsSafe isSafe, String reason) implements ToXContentObject, Writeable {
        public static Result readFrom(final StreamInput in) throws IOException {
            return new Result(IsSafe.readFrom(in), in.readString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("is_safe", isSafe.name());
            builder.field("reason", reason);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(isSafe.isSafe());
            out.writeString(reason);
        }
    }

    public enum IsSafe {
        YES("yes"),
        NO("no"),
        UNKNOWN("unknown");

        private final String isSafe;

        IsSafe(String isSafe) {
            this.isSafe = isSafe;
        }

        public String isSafe() {
            return isSafe;
        }

        public static IsSafe readFrom(final StreamInput in) throws IOException {
            return fromString(in.readString());
        }

        public static IsSafe fromString(String s) {
            return switch (s) {
                case "yes" -> YES;
                case "no" -> NO;
                case "unknown" -> UNKNOWN;
                default -> throw new IllegalArgumentException("unexpected IsSafe value [" + s + "]");
            };
        }
    }
}
