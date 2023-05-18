/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalChangePointAggregation extends InternalAggregation {

    private final ChangePointBucket bucket;
    private final ChangeType changeType;

    public InternalChangePointAggregation(String name, Map<String, Object> metadata, ChangePointBucket bucket, ChangeType changeType) {
        super(name, metadata);
        this.bucket = bucket;
        this.changeType = changeType;
    }

    public InternalChangePointAggregation(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            this.bucket = new ChangePointBucket(in);
        } else {
            this.bucket = null;
        }
        this.changeType = in.readNamedWriteable(ChangeType.class);
    }

    public ChangePointBucket getBucket() {
        return bucket;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    @Override
    public String getWriteableName() {
        return ChangePointAggregationBuilder.NAME.getPreferredName();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (bucket != null) {
            out.writeBoolean(true);
            bucket.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeNamedWriteable(changeType);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        throw new UnsupportedOperationException("Reducing a change_point aggregation is not supported");
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.size() == 1) {
            String property = path.get(0);
            if (property.equals("p_value")) {
                return changeType.pValue();
            }
            if (property.equals("type")) {
                return changeType.getName();
            }
            if (property.equals("change_point")) {
                return changeType.changePoint();
            }
        } else if (path.size() > 1 && path.get(0).equals("bucket") && bucket != null) {
            return bucket.getProperty(name, path.subList(1, path.size()));
        }
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (bucket != null) {
            builder.field("bucket", bucket);
        }
        NamedXContentObjectHelper.writeNamedObject(builder, params, "type", changeType);
        return builder;
    }
}
