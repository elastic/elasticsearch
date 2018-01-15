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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.search.SortField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link CompositeValuesSourceBuilder} that that builds a {@link HistogramValuesSource} from another numeric values source
 * using the provided interval.
 */
public class HistogramValuesSourceBuilder extends CompositeValuesSourceBuilder<HistogramValuesSourceBuilder> {
    static final String TYPE = "histogram";

    private static final ObjectParser<HistogramValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(HistogramValuesSourceBuilder.TYPE);
        PARSER.declareDouble(HistogramValuesSourceBuilder::interval, Histogram.INTERVAL_FIELD);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER, ValueType.NUMERIC);
    }
    static HistogramValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new HistogramValuesSourceBuilder(name), null);
    }

    private double interval = 0;

    public HistogramValuesSourceBuilder(String name) {
        super(name, ValueType.DOUBLE);
    }

    protected HistogramValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.interval = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(interval);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(interval);
    }

    @Override
    protected boolean innerEquals(HistogramValuesSourceBuilder other) {
        return Objects.equals(interval, other.interval);
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Returns the interval that is set on this source
     **/
    public double interval() {
        return interval;
    }

    /**
     * Sets the interval on this source.
     **/
    public HistogramValuesSourceBuilder interval(double interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be greater than 0 for [histogram] source");
        }
        this.interval = interval;
        return this;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(SearchContext context,
                                                     ValuesSourceConfig<?> config,
                                                     int pos,
                                                     int numPos,
                                                     SortField sortField) throws IOException {
        ValuesSource orig = config.toValuesSource(context.getQueryShardContext());
        if (orig == null) {
            orig = ValuesSource.Numeric.EMPTY;
        }
        if (orig instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric numeric = (ValuesSource.Numeric) orig;
            HistogramValuesSource vs = new HistogramValuesSource(numeric, interval);
            boolean canEarlyTerminate = false;
            final FieldContext fieldContext = config.fieldContext();
            if (sortField != null &&
                    pos == numPos-1 &&
                    fieldContext != null)  {
                canEarlyTerminate = checkCanEarlyTerminate(context.searcher().getIndexReader(),
                    fieldContext.field(), order() == SortOrder.ASC ? false : true, sortField);
            }
            return new CompositeValuesSourceConfig(name, vs, order(), canEarlyTerminate);
        } else {
            throw new IllegalArgumentException("invalid source, expected numeric, got " + orig.getClass().getSimpleName());
        }
    }
}
