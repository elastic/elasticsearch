/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.downsample.DownsampleDateHistogramConfig;

import java.io.IOException;
import java.util.Objects;

public class DownSampleAction extends ActionType<AcknowledgedResponse> {
    public static final DownSampleAction INSTANCE = new DownSampleAction();
    public static final String NAME = "indices:admin/xpack/downsample";

    private DownSampleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<DownSampleAction.Request> implements IndicesRequest, ToXContentObject {
        private String sourceIndex;
        private String downsampleIndex;
        private DownsampleDateHistogramConfig downsampleConfig;

        public Request(String sourceIndex, String downsampleIndex, DownsampleDateHistogramConfig downsampleConfig) {
            this.sourceIndex = sourceIndex;
            this.downsampleIndex = downsampleIndex;
            this.downsampleConfig = downsampleConfig;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            sourceIndex = in.readString();
            downsampleIndex = in.readString();
            downsampleConfig = new DownsampleDateHistogramConfig(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("source_index", sourceIndex);
            builder.field("downsample_index", downsampleIndex);
            downsampleConfig.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getDownsampleIndex() {
            return downsampleIndex;
        }

        public DownsampleDateHistogramConfig getDownsampleConfig() {
            return downsampleConfig;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request request = (Request) o;
            return Objects.equals(sourceIndex, request.sourceIndex)
                && Objects.equals(downsampleIndex, request.downsampleIndex)
                && Objects.equals(downsampleConfig, request.downsampleConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, downsampleIndex, downsampleConfig);
        }
    }
}
