/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.google.protobuf.InvalidProtocolBufferException;
import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalVectorTile extends InternalAggregation {
    final byte[] vectorTile;

    public InternalVectorTile(String name, byte[] vectorTile, Map<String, Object> metadata) {
        super(name, metadata);
        this.vectorTile = vectorTile;
    }

    /**
     * Read from a stream.
     */
    public InternalVectorTile(StreamInput in) throws IOException {
        super(in);
        vectorTile = in.readByteArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
       out.writeByteArray(vectorTile);
    }

    @Override
    public String getWriteableName() {
        return VectorTileAggregationBuilder.NAME;
    }

    public byte[] getVectorTile() {
        return vectorTile;
    }


    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
        for (InternalAggregation aggregation : aggregations) {
            try {
                tileBuilder.mergeFrom(((InternalVectorTile) aggregation).getVectorTile());
            } catch (InvalidProtocolBufferException ex) {
                throw new IllegalArgumentException("");
            }
        }
        return new InternalVectorTile(name, tileBuilder.build().toByteArray(), getMetadata());
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return vectorTile;
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), vectorTile);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(vectorTile));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalVectorTile that = (InternalVectorTile) obj;
        return Arrays.equals(vectorTile, that.vectorTile);
    }
}
