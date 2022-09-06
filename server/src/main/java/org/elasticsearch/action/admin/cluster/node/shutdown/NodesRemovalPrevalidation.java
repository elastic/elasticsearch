/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class NodesRemovalPrevalidation implements ToXContentFragment {

    private final Result overallResult;
    private final Map<String, Result> perNodeResult;

    public NodesRemovalPrevalidation(Result overallResult, Map<String, Result> perNodeResult) {
        this.overallResult = overallResult;
        this.perNodeResult = perNodeResult;
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

    // TODO: implement writable but throw exception?
    public record Result(IsSafe isSafe, String reason) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("is_safe", isSafe.name());
            builder.field("reason", reason);
            return builder;
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
    }
}
