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

package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.AbstractHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DerivativePipelineAggregatorBuilder extends PipelineAggregatorBuilder<DerivativePipelineAggregatorBuilder> {

    static final DerivativePipelineAggregatorBuilder PROTOTYPE = new DerivativePipelineAggregatorBuilder("", "");

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private String units;

    public DerivativePipelineAggregatorBuilder(String name, String bucketsPath) {
        this(name, new String[] { bucketsPath });
    }

    private DerivativePipelineAggregatorBuilder(String name, String[] bucketsPaths) {
        super(name, DerivativePipelineAggregator.TYPE.name(), bucketsPaths);
    }

    public DerivativePipelineAggregatorBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    public String format() {
        return format;
    }

    public DerivativePipelineAggregatorBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[gapPolicy] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    public DerivativePipelineAggregatorBuilder unit(String units) {
        if (units == null) {
            throw new IllegalArgumentException("[units] must not be null: [" + name + "]");
        }
        this.units = units;
        return this;
    }

    public DerivativePipelineAggregatorBuilder unit(DateHistogramInterval units) {
        if (units == null) {
            throw new IllegalArgumentException("[units] must not be null: [" + name + "]");
        }
        this.units = units.toString();
        return this;
    }

    public String unit() {
        return units;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        DocValueFormat formatter;
        if (format != null) {
            formatter = new DocValueFormat.Decimal(format);
        } else {
            formatter = DocValueFormat.RAW;
        }
        Long xAxisUnits = null;
        if (units != null) {
            DateTimeUnit dateTimeUnit = DateHistogramAggregatorFactory.DATE_FIELD_UNITS.get(units);
            if (dateTimeUnit != null) {
                xAxisUnits = dateTimeUnit.field().getDurationField().getUnitMillis();
            } else {
                TimeValue timeValue = TimeValue.parseTimeValue(units, null, getClass().getSimpleName() + ".unit");
                if (timeValue != null) {
                    xAxisUnits = timeValue.getMillis();
                }
            }
        }
        return new DerivativePipelineAggregator(name, bucketsPaths, formatter, gapPolicy, xAxisUnits, metaData);
    }

    @Override
    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories,
            List<PipelineAggregatorBuilder<?>> pipelineAggregatoractories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must contain a single entry for aggregation [" + name + "]");
        }
        if (!(parent instanceof AbstractHistogramAggregatorFactory<?>)) {
            throw new IllegalStateException("derivative aggregation [" + name
                    + "] must have a histogram or date_histogram as parent");
        } else {
            AbstractHistogramAggregatorFactory<?> histoParent = (AbstractHistogramAggregatorFactory<?>) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of derivative aggregation [" + name
                        + "] must have min_doc_count of 0");
            }
        }
    }

    @Override
    protected DerivativePipelineAggregatorBuilder doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
        DerivativePipelineAggregatorBuilder factory = new DerivativePipelineAggregatorBuilder(name, bucketsPaths);
        factory.format = in.readOptionalString();
        if (in.readBoolean()) {
            factory.gapPolicy = GapPolicy.readFrom(in);
        }
        factory.units = in.readOptionalString();
        return factory;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        boolean hasGapPolicy = gapPolicy != null;
        out.writeBoolean(hasGapPolicy);
        if (hasGapPolicy) {
            gapPolicy.writeTo(out);
        }
        out.writeOptionalString(units);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(DerivativeParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(DerivativeParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        if (units != null) {
            builder.field(DerivativeParser.UNIT.getPreferredName(), units);
        }
        return builder;
    }

    @Override
    protected boolean doEquals(Object obj) {
        DerivativePipelineAggregatorBuilder other = (DerivativePipelineAggregatorBuilder) obj;
        if (!Objects.equals(format, other.format)) {
            return false;
        }
        if (!Objects.equals(gapPolicy, other.gapPolicy)) {
            return false;
        }
        if (!Objects.equals(units, other.units)) {
            return false;
        }
        return true;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(format, gapPolicy, units);
    }

}