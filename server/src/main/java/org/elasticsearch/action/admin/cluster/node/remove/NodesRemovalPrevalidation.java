/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.remove;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class NodesRemovalPrevalidation implements Writeable {

    private final IsSafe isSafe;
    private final List<NodeResult> nodePrevalidations;

    public NodesRemovalPrevalidation(IsSafe isSafe, List<NodeResult> nodePrevalidations) {
        this.isSafe = isSafe;
        this.nodePrevalidations = nodePrevalidations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public record NodeResult(IsSafe isSafe, String reason) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }

    public enum IsSafe implements Writeable {
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
