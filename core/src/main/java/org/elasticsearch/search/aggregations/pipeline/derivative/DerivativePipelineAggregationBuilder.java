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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DerivativePipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<DerivativePipelineAggregationBuilder> {
    public static final String NAME = "derivative";
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private static final ParseField FORMAT_FIELD = new ParseField("format");
    private static final ParseField GAP_POLICY_FIELD = new ParseField("gap_policy");
    private static final ParseField UNIT_FIELD = new ParseField("unit");

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private String units;

    public DerivativePipelineAggregationBuilder(String name, String bucketsPath) {
        this(name, new String[] { bucketsPath });
    }

    private DerivativePipelineAggregationBuilder(String name, String[] bucketsPaths) {
        super(name, NAME, bucketsPaths);
    }

    /**
     * Read from a stream.
     */
    public DerivativePipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        format = in.readOptionalString();
        if (in.readBoolean()) {
            gapPolicy = GapPolicy.readFrom(in);
        }
        units = in.readOptionalString();
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

    public DerivativePipelineAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    public String format() {
        return format;
    }

    public DerivativePipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[gapPolicy] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    public DerivativePipelineAggregationBuilder unit(String units) {
        if (units == null) {
            throw new IllegalArgumentException("[units] must not be null: [" + name + "]");
        }
        this.units = units;
        return this;
    }

    public DerivativePipelineAggregationBuilder unit(DateHistogramInterval units) {
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
                xAxisUnits = dateTimeUnit.field(DateTimeZone.UTC).getDurationField().getUnitMillis();
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
            List<PipelineAggregationBuilder> pipelineAggregatoractories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must contain a single entry for aggregation [" + name + "]");
        }
        if (parent instanceof HistogramAggregatorFactory) {
            HistogramAggregatorFactory histoParent = (HistogramAggregatorFactory) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of derivative aggregation [" + name
                        + "] must have min_doc_count of 0");
            }
        } else if (parent instanceof DateHistogramAggregatorFactory) {
            DateHistogramAggregatorFactory histoParent = (DateHistogramAggregatorFactory) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of derivative aggregation [" + name
                        + "] must have min_doc_count of 0");
            }
        } else {
            throw new IllegalStateException("derivative aggregation [" + name
                    + "] must have a histogram or date_histogram as parent");
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(FORMAT_FIELD.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(GAP_POLICY_FIELD.getPreferredName(), gapPolicy.getName());
        }
        if (units != null) {
            builder.field(UNIT_FIELD.getPreferredName(), units);
        }
        return builder;
    }

    public static DerivativePipelineAggregationBuilder parse(String pipelineAggregatorName, QueryParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;
        String units = null;
        GapPolicy gapPolicy = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.getParseFieldMatcher().match(currentFieldName, FORMAT_FIELD)) {
                    format = parser.text();
                } else if (context.getParseFieldMatcher().match(currentFieldName, BUCKETS_PATH_FIELD)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (context.getParseFieldMatcher().match(currentFieldName, GAP_POLICY_FIELD)) {
                    gapPolicy = GapPolicy.parse(context, parser.text(), parser.getTokenLocation());
                } else if (context.getParseFieldMatcher().match(currentFieldName, UNIT_FIELD)) {
                    units = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.getParseFieldMatcher().match(currentFieldName, BUCKETS_PATH_FIELD)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " in [" + pipelineAggregatorName + "].");
            }
        }

        if (bucketsPaths == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + BUCKETS_PATH_FIELD.getPreferredName()
                    + "] for derivative aggregation [" + pipelineAggregatorName + "]");
        }

        DerivativePipelineAggregationBuilder factory =
                new DerivativePipelineAggregationBuilder(pipelineAggregatorName, bucketsPaths[0]);
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }
        if (units != null) {
            factory.unit(units);
        }
        return factory;
    }

    @Override
    protected boolean doEquals(Object obj) {
        DerivativePipelineAggregationBuilder other = (DerivativePipelineAggregationBuilder) obj;
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

    @Override
    public String getWriteableName() {
        return NAME;
    }
}