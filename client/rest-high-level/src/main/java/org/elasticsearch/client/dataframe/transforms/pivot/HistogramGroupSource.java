/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A grouping via a histogram aggregation referencing a numeric field
 */
public class HistogramGroupSource extends SingleGroupSource implements ToXContentObject {

    protected static final ParseField INTERVAL = new ParseField("interval");
    private static final ConstructingObjectParser<HistogramGroupSource, Void> PARSER =
            new ConstructingObjectParser<>("histogram_group_source", true,
                    args -> new HistogramGroupSource((String) args[0], (double) args[1]));

    static {
        PARSER.declareString(optionalConstructorArg(), FIELD);
        PARSER.declareDouble(optionalConstructorArg(), INTERVAL);
    }

    public static HistogramGroupSource fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final double interval;

    HistogramGroupSource(String field, double interval) {
        super(field);
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be greater than 0.");
        }
        this.interval = interval;
    }

    @Override
    public Type getType() {
        return Type.HISTOGRAM;
    }

    public double getInterval() {
        return interval;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        builder.field(INTERVAL.getPreferredName(), interval);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final HistogramGroupSource that = (HistogramGroupSource) other;

        return Objects.equals(this.field, that.field) &&
                Objects.equals(this.interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, interval);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String field;
        private double interval;

        /**
         * The field to reference in the histogram grouping
         * @param field The numeric field name to use in the histogram grouping
         * @return The {@link Builder} with the field set.
         */
        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        /**
         * Set the interval for the histogram aggregation
         * @param interval The numeric interval for the histogram grouping
         * @return The {@link Builder} with the interval set.
         */
        public Builder setInterval(double interval) {
            this.interval = interval;
            return this;
        }

        public HistogramGroupSource build() {
            return new HistogramGroupSource(field, interval);
        }
    }
}
