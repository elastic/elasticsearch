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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;

public class CumulativeSumPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<CumulativeSumPipelineAggregationBuilder> {
    public static final String NAME = "cumulative_sum";

    private String format;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<CumulativeSumPipelineAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME, false, (args, name) -> new CumulativeSumPipelineAggregationBuilder(name, (List<String>) args[0]));

    static {
        PARSER.declareStringArray(constructorArg(), BUCKETS_PATH_FIELD);
        PARSER.declareString(CumulativeSumPipelineAggregationBuilder::format, FORMAT);
    };

    public CumulativeSumPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME, new String[] { bucketsPath });
    }

    CumulativeSumPipelineAggregationBuilder(String name, List<String> bucketsPath) {
        super(name, NAME, bucketsPath.toArray(new String[0]));
    }

    /**
     * Read from a stream.
     */
    public CumulativeSumPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
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
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new CumulativeSumPipelineAggregator(name, bucketsPaths, formatter(), metadata);
    }

    @Override
    protected void validate(ValidationContext context) {
        if (bucketsPaths.length != 1) {
            context.addBucketPathValidationError("must contain a single entry for aggregation [" + name + "]");
        }

        //FIXME: duplicated from context.validate
        AggregationBuilder parent = context.getParent();
        if ( parent == null ){
            context.addValidationError(type + " aggregation [" + name
                + "] must have a histogram, date_histogram, auto_date_histogram or terms as parent but doesn't have a parent");

        } else if ( parent instanceof HistogramAggregationBuilder) {
            HistogramAggregationBuilder histoParent = (HistogramAggregationBuilder) parent;
            if (histoParent.minDocCount() != 0) {
                context.addValidationError(
                    "parent histogram of " + NAME + " aggregation [" + name + "] must have min_doc_count of 0");
            }
        } else if (parent instanceof DateHistogramAggregationBuilder) {
            DateHistogramAggregationBuilder histoParent = (DateHistogramAggregationBuilder) parent;
            if (histoParent.minDocCount() != 0) {
                context.addValidationError(
                    "parent histogram of " + NAME + " aggregation [" + name + "] must have min_doc_count of 0");
            }
        } else if (parent instanceof AutoDateHistogramAggregationBuilder) {
            // Nothing to check
        } else if (parent instanceof TermsAggregationBuilder) {
            //TODO: what should be checked?
        } else {
            context.addValidationError(
                NAME + " aggregation [" + name + "] must have a histogram, date_histogram, auto_date_histogram or terms"
                    + " as parent");
        }
    }



    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(BucketMetricsParser.FORMAT.getPreferredName(), format);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        CumulativeSumPipelineAggregationBuilder other = (CumulativeSumPipelineAggregationBuilder) obj;
        return Objects.equals(format, other.format);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
