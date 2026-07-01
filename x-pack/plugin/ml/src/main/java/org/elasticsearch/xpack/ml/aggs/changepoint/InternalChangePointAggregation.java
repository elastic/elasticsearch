/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalChangePointAggregation extends InternalAggregation {

    // Multi change points. Before this version the result carried had a single optional change on the wire;
    // from this version it carries a (possibly null-containing) list of changes.
    public static final TransportVersion MULTI_CHANGE_POINT = TransportVersion.fromName("multi_change_point");

    private final List<ChangePointBucket> buckets;
    private final List<ChangeType> changeTypes;

    public InternalChangePointAggregation(
        String name,
        Map<String, Object> metadata,
        List<ChangePointBucket> buckets,
        List<ChangeType> changeTypes
    ) {
        super(name, metadata);
        assert buckets.size() == changeTypes.size()
            : "buckets [" + buckets.size() + "] and changeTypes [" + changeTypes.size() + "] must be 1-to-1";
        this.buckets = buckets;
        this.changeTypes = changeTypes;
    }

    public InternalChangePointAggregation(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
            buckets = in.readCollectionAsList(i -> i.readOptionalWriteable(ChangePointBucket::new));
            changeTypes = in.readNamedWriteableCollectionAsList(ChangeType.class);
        } else {
            // Legacy single-bucket format: one optional bucket followed by exactly one change type.
            buckets = new ArrayList<>();
            changeTypes = new ArrayList<>();
            buckets.add(in.readOptionalWriteable(ChangePointBucket::new));
            changeTypes.add(in.readNamedWriteable(ChangeType.class));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
            out.writeCollection(buckets, StreamOutput::writeOptionalWriteable);
            out.writeNamedWriteableCollection(changeTypes);
        } else {
            // Down-convert for an older peer: a single optional bucket plus exactly one (non-optional) change
            // type. The legacy reader always reads one change type, so we must never write "nothing".
            out.writeOptionalWriteable(representativeBucket());
            out.writeNamedWriteable(representativeChangeType());
        }
    }

    /**
     * The bucket of the single most significant event in the time series and {@code null} when there aren't any.
     */
    private ChangePointBucket representativeBucket() {
        int index = representativeIndex();
        return index >= 0 ? buckets.get(index) : null;
    }

    /**
     * The most significant change or {@link ChangeType.Indeterminable} when there isn't any.
     */
    private ChangeType representativeChangeType() {
        int index = representativeIndex();
        return index >= 0 ? changeTypes.get(index) : new ChangeType.Indeterminable("no change type available");
    }

    private int representativeIndex() {
        int minPValueIndex = -1;
        double minPValue = 2.0; // Any number greater than 1.0 is sufficient.
        for (int i = 0; i < changeTypes.size(); i++) {
            ChangeType changeType = changeTypes.get(i);
            if (changeType.pValue() < minPValue) {
                minPValue = changeType.pValue();
                minPValueIndex = i;
            }
        }
        return minPValueIndex;
    }

    public List<ChangePointBucket> getBuckets() {
        return buckets;
    }

    public List<ChangeType> getChangeTypes() {
        return changeTypes;
    }

    public ChangePointBucket getBucket() {
        return representativeBucket();
    }

    public ChangeType getChangeType() {
        // For the aggregation we're going to switch to reporting the most significant bucket to avoid the backwards
        // compatibility issues. ES|QL is our preferred method of consuming change points which returns every change.
        return representativeChangeType();
    }

    @Override
    public String getWriteableName() {
        return ChangePointAggregationBuilder.NAME.getPreferredName();
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
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
                return getChangeType().pValue();
            }
            if (property.equals("type")) {
                return getChangeType().getName();
            }
            if (property.equals("change_point")) {
                return getChangeType().changePoint();
            }
        } else if (path.size() > 1 && path.get(0).equals("bucket") && getBucket() != null) {
            return getBucket().getProperty(name, path.subList(1, path.size()));
        }
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (getBucket() != null) {
            builder.field("bucket", getBucket());
        }
        NamedXContentObjectHelper.writeNamedObject(builder, params, "type", getChangeType());
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass() || super.equals(obj) == false) {
            return false;
        }
        InternalChangePointAggregation other = (InternalChangePointAggregation) obj;
        return Objects.equals(buckets, other.buckets) && Objects.equals(changeTypes, other.changeTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, changeTypes);
    }
}
