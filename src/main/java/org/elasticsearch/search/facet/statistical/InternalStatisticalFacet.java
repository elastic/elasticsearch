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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class InternalStatisticalFacet extends InternalFacet implements StatisticalFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("statistical"));

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readStatisticalFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    private double min;
    private double max;
    private double total;
    private double sumOfSquares;
    private long count;

    private InternalStatisticalFacet() {
    }

    public InternalStatisticalFacet(String name, double min, double max, double total, double sumOfSquares, long count) {
        super(name);
        this.min = min;
        this.max = max;
        this.total = total;
        this.sumOfSquares = sumOfSquares;
        this.count = count;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public long getCount() {
        return this.count;
    }

    @Override
    public double getTotal() {
        return this.total;
    }

    @Override
    public double getSumOfSquares() {
        return this.sumOfSquares;
    }

    @Override
    public double getMean() {
        if (count == 0) {
            return 0;
        }
        return total / count;
    }

    @Override
    public double getMin() {
        return this.min;
    }

    @Override
    public double getMax() {
        return this.max;
    }

    public double getVariance() {
        return (sumOfSquares - ((total * total) / count)) / count;
    }

    public double getStdDeviation() {
        return Math.sqrt(getVariance());
    }

    @Override
    public Facet reduce(ReduceContext context) {
        List<Facet> facets = context.facets();
        if (facets.size() == 1) {
            return facets.get(0);
        }
        double min = Double.NaN;
        double max = Double.NaN;
        double total = 0;
        double sumOfSquares = 0;
        long count = 0;

        for (Facet facet : facets) {
            InternalStatisticalFacet statsFacet = (InternalStatisticalFacet) facet;
            if (statsFacet.getMin() < min || Double.isNaN(min)) {
                min = statsFacet.getMin();
            }
            if (statsFacet.getMax() > max || Double.isNaN(max)) {
                max = statsFacet.getMax();
            }
            total += statsFacet.getTotal();
            sumOfSquares += statsFacet.getSumOfSquares();
            count += statsFacet.getCount();
        }

        return new InternalStatisticalFacet(getName(), min, max, total, sumOfSquares, count);
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
        builder.startObject(getName());
        builder.field(Fields._TYPE, StatisticalFacet.TYPE);
        builder.field(Fields.COUNT, getCount());
        builder.field(Fields.TOTAL, getTotal());
        builder.field(Fields.MIN, getMin());
        builder.field(Fields.MAX, getMax());
        builder.field(Fields.MEAN, getMean());
        builder.field(Fields.SUM_OF_SQUARES, getSumOfSquares());
        builder.field(Fields.VARIANCE, getVariance());
        builder.field(Fields.STD_DEVIATION, getStdDeviation());
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
        super.readFrom(in);
        count = in.readVLong();
        total = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
        sumOfSquares = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(count);
        out.writeDouble(total);
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(sumOfSquares);
    }
}
