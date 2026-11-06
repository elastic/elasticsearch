/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.explain;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Explains the downsampling of indices, like the lifecycle explain API does for ILM: for each requested index that is the target
 * of a downsample, where the operation stands as a whole and on every shard. It reads the downsample state index, so the details
 * are only available for operations orchestrated through it; for the others only the downsample status of the index is known.
 * <p>
 * The action runs on the node that receives the request and never goes over the wire.
 */
public class ExplainDownsampleAction extends ActionType<ExplainDownsampleAction.Response> {

    public static final ExplainDownsampleAction INSTANCE = new ExplainDownsampleAction();
    public static final String NAME = "indices:admin/xpack/downsample/explain";

    private ExplainDownsampleAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private String[] indices = Strings.EMPTY_ARRAY;
        // downsample targets are hidden until the downsample succeeds
        private IndicesOptions indicesOptions = IndicesOptions.strictExpandHidden();
        private String[] sourceIndices = Strings.EMPTY_ARRAY;

        public Request(TimeValue masterTimeout) {
            super(masterTimeout);
        }

        @Override
        public Request indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        /**
         * Only explain the downsamples of these source indices; all of them if empty.
         */
        public String[] sourceIndices() {
            return sourceIndices;
        }

        public Request sourceIndices(String... sourceIndices) {
            this.sourceIndices = sourceIndices == null ? Strings.EMPTY_ARRAY : sourceIndices;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Request other
                && Arrays.equals(indices, other.indices)
                && Objects.equals(indicesOptions, other.indicesOptions)
                && Arrays.equals(sourceIndices, other.sourceIndices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), indicesOptions, Arrays.hashCode(sourceIndices));
        }
    }

    /**
     * One entry per downsampled index, sorted by index name. Never sent to another node.
     */
    public static final class Response extends ActionResponse implements ToXContentObject {

        private final List<IndexDownsampleExplanation> indices;

        public Response(List<IndexDownsampleExplanation> indices) {
            this.indices = List.copyOf(indices);
        }

        public List<IndexDownsampleExplanation> indices() {
            return indices;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("indices");
            for (IndexDownsampleExplanation index : indices) {
                builder.field(index.index());
                index.toXContent(builder, params);
            }
            builder.endObject();
            return builder.endObject();
        }
    }
}
