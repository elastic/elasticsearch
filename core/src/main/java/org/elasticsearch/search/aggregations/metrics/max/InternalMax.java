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
package org.elasticsearch.search.aggregations.metrics.max;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalMax extends InternalNumericMetricsAggregation.SingleValue implements Max {
    private final double max;

    public InternalMax(String name, double max, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.format = formatter;
        this.max = max;
    }

    /**
     * Read from a stream.
     */
    public InternalMax(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        max = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(max);
    }

    @Override
    public String getWriteableName() {
        return MaxAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return max;
    }

    @Override
    public double getValue() {
        return max;
    }

    @Override
    public InternalMax doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double max = Double.NEGATIVE_INFINITY;
        for (InternalAggregation aggregation : aggregations) {
            max = Math.max(max, ((InternalMax) aggregation).max);
        }
        return new InternalMax(name, max, format, pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !Double.isInfinite(max);
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? max : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(max));
        }
        return builder;
    }

    private static final ObjectParser<Map<String, Object>, Void> PARSER = new ObjectParser<>(
            "internal_max", HashMap::new);

    static {
        declareCommonField(PARSER);
        PARSER.declareField((map, value) -> map.put(CommonFields.VALUE.getPreferredName(), value),
                (p, c) -> InternalMax.parseValue(p, Double.NEGATIVE_INFINITY), CommonFields.VALUE,
                ValueType.DOUBLE_OR_NULL);
        PARSER.declareString((map, valueAsString) -> map.put(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString),
                CommonFields.VALUE_AS_STRING);
    }

    @SuppressWarnings("unchecked")
    public static InternalMax parseXContentBody(String name, XContentParser parser) {
        Map<String, Object> map = PARSER.apply(parser, null);
        double max = (Double) map.getOrDefault(CommonFields.VALUE.getPreferredName(), Double.NEGATIVE_INFINITY);
        String valueAsString = (String) map.get(CommonFields.VALUE_AS_STRING.getPreferredName());
        Map<String, Object> metaData = (Map<String, Object>) map.get(CommonFields.META.getPreferredName());
        DocValueFormat formatter = DocValueFormat.RAW;
        if (valueAsString != null) {
            formatter = InternalNumericMetricsAggregation.wrapStringsInFormatter(Collections.singletonMap(max, valueAsString));
        }
        return new InternalMax(name, max, formatter, Collections.emptyList(), metaData);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(max);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalMax other = (InternalMax) obj;
        return Objects.equals(max, other.max);
    }
}
