/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalHllSketchAggregation extends InternalNumericMetricsAggregation.SingleValue implements HllSketchAggregation {

    private HllSketch valueSketch;

    public InternalHllSketchAggregation(String name, HllSketch valueSketch, Map<String, Object> metadata) {
        super(name, metadata);
        this.valueSketch = valueSketch;
    }

    public InternalHllSketchAggregation(StreamInput in) throws IOException {
        super(in);
        this.format = in.readNamedWriteable(DocValueFormat.class);
        if (in.readBoolean()) {
            byte[] arr = in.readByteArray();
            this.valueSketch = HllSketch.heapify(arr);

        } else {
            this.valueSketch = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(this.format);
        if (this.valueSketch != null) {
            out.writeBoolean(true);
            byte[] arr = this.valueSketch.toCompactByteArray();
            out.writeByteArray(arr);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        final Union union = new Union(this.valueSketch.getLgConfigK());
        union.update(this.valueSketch);
        for (InternalAggregation aggregation : aggregations) {
            final InternalHllSketchAggregation dsCardinality = (InternalHllSketchAggregation) aggregation;
            union.update(dsCardinality.valueSketch);
        }

        return new InternalHllSketchAggregation(name, union.getResult(TgtHllType.HLL_8), this.getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

        final Double result = this.value();

        builder.field(CommonFields.VALUE.getPreferredName(), result);
        if (format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(result).toString());
        }

        return builder;
    }

    /**
     * a hash code identified this InternalHllSketchAggregation
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valueSketch);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalHllSketchAggregation other = (InternalHllSketchAggregation) obj;
        if (this.valueSketch != null && other.valueSketch != null) return this.valueSketch.equals(other.valueSketch);
        return false;
    }

    @Override
    public String getWriteableName() {
        return "hll_sketch";
    }

    /**
     * value of this aggregation
     * @return the value
     */
    @Override
    public double value() {
        return (double) getValue();
    }

    /**
     * value of this aggregation
     * @return the value
     */
    @Override
    public long getValue() {
        return this.valueSketch == null ? 0L : Math.round(this.valueSketch.getEstimate());
    }

}
