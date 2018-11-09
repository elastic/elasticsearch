/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexing;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Helper class to collect min, max, avg and total statistics for a quantity.
 *
 * Derived from ML's version.  Ttweaked to be xcontent/writeable and
 * handle "empty" scenario differently
 */
public class StatsAccumulator implements Writeable, ToXContentFragment {

    private static final String NAME = "stats_accumulator";
    private static final ParseField MIN = new ParseField("min");
    private static final ParseField MAX = new ParseField("max");
    private static final ParseField AVG = new ParseField("avg");
    private static final ParseField COUNT = new ParseField("count");
    private static final ParseField TOTAL = new ParseField("total");

    public static final ConstructingObjectParser<StatsAccumulator, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true,
            args -> new StatsAccumulator((long) args[0], (long) args[1], (long) args[2], (long) args[3]));

    static {
        PARSER.declareLong(constructorArg(), COUNT);
        PARSER.declareLong(constructorArg(), TOTAL);
        PARSER.declareLong(constructorArg(), MIN);
        PARSER.declareLong(constructorArg(), MAX);
        PARSER.declareLong(constructorArg(), AVG); // We parse but don't actually use the avg
    }

    private long count = 0;
    private long total = 0;
    private long min = 0;
    private long max = 0;

    public StatsAccumulator() {
    }

    public StatsAccumulator(StreamInput in) throws IOException {
        count = in.readVLong();
        total = in.readVLong();
        min = in.readVLong();
        max = in.readVLong();
    }

    private StatsAccumulator(long count, long total, long min, long max) {
        this.count = count;
        this.total = total;
        this.min = min;
        this.max = max;
    }

    public void add(long value) {
        min = count == 0 ? value : Math.min(min, value);
        max = count == 0 ? value : Math.max(max, value);
        count += 1;
        total += value;
    }

    public long getCount() {
        return count;
    }

    public long getMin() {
        return count == 0 ? 0 : min;
    }

    public long getMax() {
        return count == 0 ? 0 : max;
    }

    public double getAvg() {
        return count == 0 ? 0.0 : (double) total / (double) count;
    }

    public long getTotal() {
        return total;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(COUNT.getPreferredName(), count);
        builder.field(TOTAL.getPreferredName(), total);
        builder.field(MIN.getPreferredName(), getMin());
        builder.field(MAX.getPreferredName(), getMax());
        builder.field(AVG.getPreferredName(), getAvg());
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(total);
        out.writeVLong(min);
        out.writeVLong(max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, total, min, max);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        StatsAccumulator other = (StatsAccumulator) obj;
        return Objects.equals(count, other.count)
            && Objects.equals(total, other.total)
            && Objects.equals(min, other.min)
            && Objects.equals(max, other.max);
    }
}

