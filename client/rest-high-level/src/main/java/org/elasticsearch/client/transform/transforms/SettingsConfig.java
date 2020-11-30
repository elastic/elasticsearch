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

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SettingsConfig implements ToXContentObject {

    private static final ParseField MAX_PAGE_SEARCH_SIZE = new ParseField("max_page_search_size");
    private static final ParseField DOCS_PER_SECOND = new ParseField("docs_per_second");
    private static final ParseField WRITE_DATE_AS_EPOCH_MILLIS = new ParseField("write_date_as_epoch_millis");
    private static final int DEFAULT_MAX_PAGE_SEARCH_SIZE = -1;
    private static final float DEFAULT_DOCS_PER_SECOND = -1F;

    // use an integer as we need to code 4 states: true, false, null (unchanged), default (defined server side)
    private static final int DEFAULT_WRITE_DATE_AS_EPOCH_MILLIS = -1;

    private final Integer maxPageSearchSize;
    private final Float docsPerSecond;
    private final Integer writeDateAsEpochMillis;

    private static final ConstructingObjectParser<SettingsConfig, Void> PARSER = new ConstructingObjectParser<>(
        "settings_config",
        true,
        args -> new SettingsConfig((Integer) args[0], (Float) args[1], (Integer) args[2])
    );

    static {
        PARSER.declareIntOrNull(optionalConstructorArg(), DEFAULT_MAX_PAGE_SEARCH_SIZE, MAX_PAGE_SEARCH_SIZE);
        PARSER.declareFloatOrNull(optionalConstructorArg(), DEFAULT_DOCS_PER_SECOND, DOCS_PER_SECOND);
        PARSER.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? DEFAULT_WRITE_DATE_AS_EPOCH_MILLIS : p.booleanValue() ? 1 : 0,
            WRITE_DATE_AS_EPOCH_MILLIS,
            ValueType.BOOLEAN_OR_NULL
        );
    }

    public static SettingsConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    SettingsConfig(Integer maxPageSearchSize, Float docsPerSecond, Integer writeDateAsEpochMillis) {
        this.maxPageSearchSize = maxPageSearchSize;
        this.docsPerSecond = docsPerSecond;
        this.writeDateAsEpochMillis = writeDateAsEpochMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (maxPageSearchSize != null) {
            if (maxPageSearchSize.equals(DEFAULT_MAX_PAGE_SEARCH_SIZE)) {
                builder.field(MAX_PAGE_SEARCH_SIZE.getPreferredName(), (Integer) null);
            } else {
                builder.field(MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
            }
        }
        if (docsPerSecond != null) {
            if (docsPerSecond.equals(DEFAULT_DOCS_PER_SECOND)) {
                builder.field(DOCS_PER_SECOND.getPreferredName(), (Float) null);
            } else {
                builder.field(DOCS_PER_SECOND.getPreferredName(), docsPerSecond);
            }
        }
        if (writeDateAsEpochMillis != null) {
            if (writeDateAsEpochMillis.equals(DEFAULT_WRITE_DATE_AS_EPOCH_MILLIS)) {
                builder.field(WRITE_DATE_AS_EPOCH_MILLIS.getPreferredName(), (Boolean) null);
            } else {
                builder.field(WRITE_DATE_AS_EPOCH_MILLIS.getPreferredName(), writeDateAsEpochMillis > 0 ? true : false);
            }
        }
        builder.endObject();
        return builder;
    }

    public Integer getMaxPageSearchSize() {
        return maxPageSearchSize;
    }

    public Float getDocsPerSecond() {
        return docsPerSecond;
    }

    public Boolean getWriteDateAsEpochMillis() {
        return writeDateAsEpochMillis != null ? writeDateAsEpochMillis > 0 : null;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        SettingsConfig that = (SettingsConfig) other;
        return Objects.equals(maxPageSearchSize, that.maxPageSearchSize)
            && Objects.equals(docsPerSecond, that.docsPerSecond)
            && Objects.equals(writeDateAsEpochMillis, that.writeDateAsEpochMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPageSearchSize, docsPerSecond, writeDateAsEpochMillis);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Integer maxPageSearchSize;
        private Float docsPerSecond;
        private Integer writeDateAsEpochMilli;

        /**
         * Sets the paging maximum paging maxPageSearchSize that transform can use when
         * pulling the data from the source index.
         *
         * If OOM is triggered, the paging maxPageSearchSize is dynamically reduced so that the transform can continue to gather data.
         *
         * @param maxPageSearchSize Integer value between 10 and 10_000
         * @return the {@link Builder} with the paging maxPageSearchSize set.
         */
        public Builder setMaxPageSearchSize(Integer maxPageSearchSize) {
            this.maxPageSearchSize = maxPageSearchSize == null ? DEFAULT_MAX_PAGE_SEARCH_SIZE : maxPageSearchSize;
            return this;
        }

        /**
         * Sets the docs per second that transform can use when pulling the data from the source index.
         *
         * This setting throttles transform by issuing queries less often, however processing still happens in
         * batches. A value of 0 disables throttling (default).
         *
         * @param docsPerSecond Integer value
         * @return the {@link Builder} with requestsPerSecond set.
         */
        public Builder setRequestsPerSecond(Float docsPerSecond) {
            this.docsPerSecond = docsPerSecond == null ? DEFAULT_DOCS_PER_SECOND : docsPerSecond;
            return this;
        }

        /**
         * Sets whether to write the output of a date aggregation as millis since epoch or as formatted string (ISO format).
         *
         * This setting ensures backwards compatibility for transforms created before 7.11, which wrote dates as epoch_millis.
         * The new default is ISO string.
         *
         * @param writeDateAsEpochMilli true if dates should be written as epoch_millis.
         * @return the {@link Builder} with writeDateAsEpochMilli set.
         */
        public Builder setWriteDateAsEpochMilli(Boolean writeDateAsEpochMilli) {
            this.writeDateAsEpochMilli = writeDateAsEpochMilli == null ? DEFAULT_WRITE_DATE_AS_EPOCH_MILLIS : writeDateAsEpochMilli ? 1 : 0;
            return this;
        }

        public SettingsConfig build() {
            return new SettingsConfig(maxPageSearchSize, docsPerSecond, writeDateAsEpochMilli);
        }
    }
}
