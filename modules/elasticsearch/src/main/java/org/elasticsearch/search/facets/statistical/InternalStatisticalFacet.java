/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.search.facets.statistical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.internal.InternalFacet;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class InternalStatisticalFacet implements StatisticalFacet, InternalFacet {

    private String name;

    private String fieldName;

    private double min;

    private double max;

    private double total;

    private double sumOfSquares;

    private long count;

    private InternalStatisticalFacet() {
    }

    public InternalStatisticalFacet(String name, String fieldName, double min, double max, double total, double sumOfSquares, long count) {
        this.name = name;
        this.fieldName = fieldName;
        this.min = min;
        this.max = max;
        this.total = total;
        this.sumOfSquares = sumOfSquares;
        this.count = count;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String getName() {
        return name();
    }

    @Override public String fieldName() {
        return this.fieldName;
    }

    @Override public String getFieldName() {
        return fieldName();
    }

    @Override public Type type() {
        return Type.STATISTICAL;
    }

    @Override public Type getType() {
        return type();
    }

    @Override public long count() {
        return this.count;
    }

    @Override public long getCount() {
        return count();
    }

    @Override public double total() {
        return this.total;
    }

    @Override public double getTotal() {
        return total();
    }

    @Override public double sumOfSquares() {
        return this.sumOfSquares;
    }

    @Override public double getSumOfSquares() {
        return sumOfSquares();
    }

    @Override public double mean() {
        return total / count;
    }

    @Override public double getMean() {
        return mean();
    }

    @Override public double min() {
        return this.min;
    }

    @Override public double getMin() {
        return min();
    }

    @Override public double max() {
        return this.max;
    }

    @Override public double getMax() {
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

    @Override public Facet aggregate(Iterable<Facet> facets) {
        double min = Double.NaN;
        double max = Double.NaN;
        double total = 0;
        double sumOfSquares = 0;
        long count = 0;

        for (Facet facet : facets) {
            if (!facet.name().equals(name)) {
                continue;
            }
            InternalStatisticalFacet statsFacet = (InternalStatisticalFacet) facet;
            if (statsFacet.min() < min || Double.isNaN(min)) {
                min = statsFacet.min();
            }
            if (statsFacet.max() > max || Double.isNaN(max)) {
                max = statsFacet.max();
            }
            total += statsFacet.total();
            sumOfSquares += statsFacet.sumOfSquares();
            count += statsFacet.count();
        }

        return new InternalStatisticalFacet(name, fieldName, min, max, total, sumOfSquares, count);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("_type", "statistical");
        builder.field("_field", fieldName);
        builder.field("count", count());
        builder.field("total", total());
        builder.field("min", min());
        builder.field("max", max());
        builder.field("mean", mean());
        builder.field("sum_of_squares", sumOfSquares());
        builder.field("variance", variance());
        builder.field("std_deviation", stdDeviation());
        builder.endObject();
    }

    public static StatisticalFacet readStatisticalFacet(StreamInput in) throws IOException {
        InternalStatisticalFacet facet = new InternalStatisticalFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        fieldName = in.readUTF();
        count = in.readVLong();
        total = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
        sumOfSquares = in.readDouble();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(fieldName);
        out.writeVLong(count);
        out.writeDouble(total);
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(sumOfSquares);
    }
}
