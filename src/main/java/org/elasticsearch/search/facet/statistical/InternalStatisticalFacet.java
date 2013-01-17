/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.facet.statistical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;

/**
 *
 */
public class InternalStatisticalFacet implements StatisticalFacet, InternalFacet {

    private static final String STREAM_TYPE = "statistical";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readStatisticalFacet(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    private String name;

    private double min;

    private double max;

    private double total;

    private double sumOfSquares;

    private long count;

    private InternalStatisticalFacet() {
    }

    public InternalStatisticalFacet(String name, double min, double max, double total, double sumOfSquares, long count) {
        this.name = name;
        this.min = min;
        this.max = max;
        this.total = total;
        this.sumOfSquares = sumOfSquares;
        this.count = count;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public long count() {
        return this.count;
    }

    @Override
    public long getCount() {
        return count();
    }

    @Override
    public double total() {
        return this.total;
    }

    @Override
    public double getTotal() {
        return total();
    }

    @Override
    public double sumOfSquares() {
        return this.sumOfSquares;
    }

    @Override
    public double getSumOfSquares() {
        return sumOfSquares();
    }

    @Override
    public double mean() {
        if (count == 0) {
            return 0;
        }
        return total / count;
    }

    @Override
    public double getMean() {
        return mean();
    }

    @Override
    public double min() {
        return this.min;
    }

    @Override
    public double getMin() {
        return min();
    }

    @Override
    public double max() {
        return this.max;
    }

    @Override
    public double getMax() {
        return max();
    }

    public double variance() {
        return (sumOfSquares - ((total * total) / count)) / count;
    }

    public double getVariance() {
        return variance();
    }

    public double stdDeviation() {
        return Math.sqrt(variance());
    }

    public double getStdDeviation() {
        return stdDeviation();
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString SUM_OF_SQUARES = new XContentBuilderString("sum_of_squares");
        static final XContentBuilderString VARIANCE = new XContentBuilderString("variance");
        static final XContentBuilderString STD_DEVIATION = new XContentBuilderString("std_deviation");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, StatisticalFacet.TYPE);
        builder.field(Fields.COUNT, count());
        builder.field(Fields.TOTAL, total());
        builder.field(Fields.MIN, min());
        builder.field(Fields.MAX, max());
        builder.field(Fields.MEAN, mean());
        builder.field(Fields.SUM_OF_SQUARES, sumOfSquares());
        builder.field(Fields.VARIANCE, variance());
        builder.field(Fields.STD_DEVIATION, stdDeviation());
        builder.endObject();
        return builder;
    }

    public static StatisticalFacet readStatisticalFacet(StreamInput in) throws IOException {
        InternalStatisticalFacet facet = new InternalStatisticalFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        count = in.readVLong();
        total = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
        sumOfSquares = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(count);
        out.writeDouble(total);
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(sumOfSquares);
    }
}
