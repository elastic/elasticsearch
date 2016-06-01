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

package org.elasticsearch.search.aggregations.pipeline.cumulativesum;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.AbstractHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;

public class CumulativeSumPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<CumulativeSumPipelineAggregationBuilder> {
    public static final String NAME = CumulativeSumPipelineAggregator.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private String format;

    public CumulativeSumPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, CumulativeSumPipelineAggregator.TYPE.name(), new String[] { bucketsPath });
    }

    /**
     * Read from a stream.
     */
    public CumulativeSumPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, CumulativeSumPipelineAggregator.TYPE.name());
        format = in.readOptionalString();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public CumulativeSumPipelineAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    /**
     * Gets the format to use on the output of this aggregation.
     */
    public String format() {
        return format;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new CumulativeSumPipelineAggregator(name, bucketsPaths, formatter(), metaData);
    }

    @Override
    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories,
            List<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(BUCKETS_PATH.getPreferredName()
                    + " must contain a single entry for aggregation [" + name + "]");
        }
        if (!(parent instanceof AbstractHistogramAggregatorFactory<?>)) {
            throw new IllegalStateException("cumulative sum aggregation [" + name
                    + "] must have a histogram or date_histogram as parent");
        } else {
            AbstractHistogramAggregatorFactory<?> histoParent = (AbstractHistogramAggregatorFactory<?>) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of cumulative sum aggregation [" + name
                        + "] must have min_doc_count of 0");
            }
        }
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(BucketMetricsParser.FORMAT.getPreferredName(), format);
        }
        return builder;
    }

    public static CumulativeSumPipelineAggregationBuilder parse(String pipelineAggregatorName, QueryParseContext context)
            throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.getParseFieldMatcher().match(currentFieldName, FORMAT)) {
                    format = parser.text();
                } else if (context.getParseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    bucketsPaths = new String[] { parser.text() };
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.getParseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
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
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for derivative aggregation [" + pipelineAggregatorName + "]");
        }

        CumulativeSumPipelineAggregationBuilder factory =
                new CumulativeSumPipelineAggregationBuilder(pipelineAggregatorName, bucketsPaths[0]);
        if (format != null) {
            factory.format(format);
        }
        return factory;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(format);
    }

    @Override
    protected boolean doEquals(Object obj) {
        CumulativeSumPipelineAggregationBuilder other = (CumulativeSumPipelineAggregationBuilder) obj;
        return Objects.equals(format, other.format);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}