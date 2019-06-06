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
package org.elasticsearch.client.ml.job.stats;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Helper class for min, max, avg and total statistics for a quantity
 */
public class SimpleStats implements ToXContentObject {

    public static final ParseField MIN = new ParseField("min");
    public static final ParseField MAX = new ParseField("max");
    public static final ParseField AVG = new ParseField("avg");
    public static final ParseField TOTAL = new ParseField("total");

    public static final ConstructingObjectParser<SimpleStats, Void> PARSER = new ConstructingObjectParser<>("simple_stats", true,
        (a) -> {
        int i = 0;
        double total = (double)a[i++];
            double min = (double)a[i++];
            double max = (double)a[i++];
            double avg = (double)a[i++];
            return new SimpleStats(total, min, max, avg);
        });

    static {
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), TOTAL);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MIN);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MAX);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), AVG);
    }

    private final double total;
    private final double min;
    private final double max;
    private final double avg;

    SimpleStats(double total, double min, double max, double avg) {
        this.total = total;
        this.min = min;
        this.max = max;
        this.avg = avg;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAvg() {
        return avg;
    }

    public double getTotal() {
        return total;
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, min, max, avg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SimpleStats other = (SimpleStats) obj;
        return Objects.equals(total, other.total) &&
            Objects.equals(min, other.min) &&
            Objects.equals(avg, other.avg) &&
            Objects.equals(max, other.max);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MIN.getPreferredName(), min);
        builder.field(MAX.getPreferredName(), max);
        builder.field(AVG.getPreferredName(), avg);
        builder.field(TOTAL.getPreferredName(), total);
        builder.endObject();
        return builder;
    }
}

