/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample.action;

import java.io.IOException;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.downsample.DownsampleDateHistogramConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;

public class DownSampleAction extends ActionType<AcknowledgedResponse> {
    public static final DownSampleAction INSTANCE = new DownSampleAction();
    public static final String NAME = "indices:admin/xpack/downsample";

    private DownSampleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    //public static class Request extends MasterNodeRequest<RollupAction.Request> implements IndicesRequest, ToXContentObject {
    //    private String sourceIndex;
    //    private String rollupIndex;
    //    private DownsampleDateHistogramConfig downsampleConfig;
    //
    //    public Request(String sourceIndex, String rollupIndex, DownsampleDateHistogramConfig downsampleConfig) {
    //        this.sourceIndex = sourceIndex;
    //        this.rollupIndex = rollupIndex;
    //        this.downsampleConfig = downsampleConfig;
    //    }
    //
    //    public Request() {}
    //
    //    public Request(StreamInput in) throws IOException {
    //        super(in);
    //        sourceIndex = in.readString();
    //        rollupIndex = in.readString();
    //        downsampleConfig = new RollupActionConfig(in);
    //    }
    //
    //    @Override
    //    public String[] indices() {
    //        return new String[] {sourceIndex};
    //    }
    //
    //    @Override
    //    public IndicesOptions indicesOptions() {
    //        return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
    //    }
    //
    //}
}
