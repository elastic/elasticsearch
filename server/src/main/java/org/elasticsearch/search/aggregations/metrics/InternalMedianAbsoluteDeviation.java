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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregator.computeMedianAbsoluteDeviation;

public class InternalMedianAbsoluteDeviation extends InternalNumericMetricsAggregation.SingleValue implements MedianAbsoluteDeviation {

    protected final TDigestState valuesSketch;

    public InternalMedianAbsoluteDeviation(String name,
                                           List<PipelineAggregator> pipelineAggregators,
                                           Map<String, Object> metaData,
                                           DocValueFormat format,
                                           TDigestState valuesSketch) {

        super(name, pipelineAggregators, metaData);
        this.format = format;
        this.valuesSketch = valuesSketch;
    }

    public InternalMedianAbsoluteDeviation(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        valuesSketch = TDigestState.read(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        TDigestState.write(valuesSketch, out);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        TDigestState valueMerged = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalMedianAbsoluteDeviation magAgg = (InternalMedianAbsoluteDeviation) aggregation;
            if (valueMerged == null) {
                valueMerged = new TDigestState(magAgg.valuesSketch.compression());
            }
            valueMerged.add(magAgg.valuesSketch);
        }

        return new InternalMedianAbsoluteDeviation(name, pipelineAggregators(), metaData, format, valueMerged);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final boolean anyResults = valuesSketch.size() > 0;
        final Double mad = anyResults
            ? getMedianAbsoluteDeviation()
            : null;

        builder.field(CommonFields.VALUE.getPreferredName(), mad);
        if (format != DocValueFormat.RAW && anyResults) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(mad).toString());
        }

        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(valuesSketch);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalMedianAbsoluteDeviation other = (InternalMedianAbsoluteDeviation) obj;
        return Objects.equals(valuesSketch, other.valuesSketch);
    }

    @Override
    public String getWriteableName() {
        return MedianAbsoluteDeviationAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getMedianAbsoluteDeviation();
    }

    @Override
    public double getMedianAbsoluteDeviation() {
        return computeMedianAbsoluteDeviation(valuesSketch);
    }
}
