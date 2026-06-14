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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalChangePointAggregation extends InternalAggregation {

    // Multi-bucket change points. Before this version the result carried a single optional bucket and a single
    // change type on the wire; from this version it carries a (possibly null-containing) list of buckets and a
    // list of change types. The negotiated transport version is min(local, remote), so an older peer downgrades
    // both sides to the single-bucket format automatically (see doWriteTo / the StreamInput constructor).
    public static final TransportVersion CHANGE_POINT_MULTI_BUCKET = TransportVersion.fromName("change_point_multi_bucket");

    private final List<ChangePointBucket> buckets;
    private final List<ChangeType> changeTypes;

    public InternalChangePointAggregation(
        String name,
        Map<String, Object> metadata,
        List<ChangePointBucket> buckets,
        List<ChangeType> changeTypes
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.changeTypes = changeTypes;
    }

    public InternalChangePointAggregation(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(CHANGE_POINT_MULTI_BUCKET)) {
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
        if (out.getTransportVersion().supports(CHANGE_POINT_MULTI_BUCKET)) {
            out.writeCollection(buckets, StreamOutput::writeOptionalWriteable);
            out.writeNamedWriteableCollection(changeTypes);
        } else {
            // Down-convert for an older peer: a single optional bucket plus exactly one (non-optional) change
            // type. The legacy reader always reads one change type, so we must never write "nothing".
            out.writeOptionalWriteable(buckets.isEmpty() ? null : buckets.get(0));
            out.writeNamedWriteable(representativeChangeType());
        }
    }

    /**
     * The single change type an older node expects when the multi-bucket result is collapsed to one: the most
     * significant (smallest p-value). Falls back to {@link ChangeType.Indeterminable} when there is none, because
     * the legacy wire format has no "absent change type" encoding.
     */
    private ChangeType representativeChangeType() {
        return changeTypes.stream()
            .min(Comparator.comparingDouble(ChangeType::pValue))
            .orElseGet(() -> new ChangeType.Indeterminable("no change type available"));
    }

    public List<ChangePointBucket> getBuckets() {
        return buckets;
    }

    public List<ChangeType> getChangeTypes() {
        return changeTypes;
    }

    // TODO multi-bucket: getProperty / doXContentBody still surface only the first bucket/change type. Revisit
    // when the multi-bucket REST representation is designed.
    public ChangePointBucket getBucket() {
        return buckets.isEmpty() ? null : buckets.get(0);
    }

    public ChangeType getChangeType() {
        return changeTypes.isEmpty() ? new ChangeType.Indeterminable("no change type available") : changeTypes.get(0);
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
