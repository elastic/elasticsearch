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

import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.internal.InternalFacet;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class InternalStatisticalFacet implements StatisticalFacet, InternalFacet {

    private String name;

    private double min;

    private double max;

    private double total;

    private long count;

    private InternalStatisticalFacet() {
    }

    public InternalStatisticalFacet(String name, double min, double max, double total, long count) {
        this.name = name;
        this.min = min;
        this.max = max;
        this.total = total;
        this.count = count;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String getName() {
        return name();
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

    @Override public Facet aggregate(Iterable<Facet> facets) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double total = 0;
        long count = 0;

        for (Facet facet : facets) {
            if (!facet.name().equals(name)) {
                continue;
            }
            InternalStatisticalFacet statsFacet = (InternalStatisticalFacet) facet;
            if (statsFacet.min() < min) {
                min = statsFacet.min();
            }
            if (statsFacet.max() > max) {
                max = statsFacet.max();
            }
            total += statsFacet.total();
            count += statsFacet.count();
        }

        return new InternalStatisticalFacet(name, min, max, total, count);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("_type", "statistical");
        builder.field("count", count);
        builder.field("total", total);
        builder.field("min", min);
        builder.field("max", max);
        builder.endObject();
    }

    public static StatisticalFacet readStatisticalFacet(StreamInput in) throws IOException {
        InternalStatisticalFacet facet = new InternalStatisticalFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        count = in.readVLong();
        total = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVLong(count);
        out.writeDouble(total);
        out.writeDouble(min);
        out.writeDouble(max);
    }
}
