/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.latest;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Class describing how to compute latest doc for every unique key
 */
public class LatestConfig implements ToXContentObject {

    private static final String NAME = "latest_config";

    private static final ParseField UNIQUE_KEY = new ParseField("unique_key");
    private static final ParseField SORT = new ParseField("sort");

    private final List<String> uniqueKey;
    private final String sort;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<LatestConfig, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new LatestConfig((List<String>) args[0], (String) args[1])
    );

    static {
        PARSER.declareStringArray(constructorArg(), UNIQUE_KEY);
        PARSER.declareString(constructorArg(), SORT);
    }

    public static LatestConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    LatestConfig(List<String> uniqueKey, String sort) {
        this.uniqueKey = uniqueKey;
        this.sort = sort;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(UNIQUE_KEY.getPreferredName(), uniqueKey);
        builder.field(SORT.getPreferredName(), sort);
        builder.endObject();
        return builder;
    }

    public List<String> getUniqueKey() {
        return uniqueKey;
    }

    public String getSort() {
        return sort;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        LatestConfig that = (LatestConfig) other;
        return Objects.equals(this.uniqueKey, that.uniqueKey) && Objects.equals(this.sort, that.sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueKey, sort);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> uniqueKey;
        private String sort;

        /**
         * Set how to group the source data
         * @param uniqueKey The configuration describing how to group the source data
         * @return the {@link Builder} with the unique key set.
         */
        public Builder setUniqueKey(String... uniqueKey) {
            return setUniqueKey(Arrays.asList(uniqueKey));
        }

        public Builder setUniqueKey(List<String> uniqueKey) {
            this.uniqueKey = uniqueKey;
            return this;
        }

        public Builder setSort(String sort) {
            this.sort = sort;
            return this;
        }

        public LatestConfig build() {
            return new LatestConfig(uniqueKey, sort);
        }
    }
}
