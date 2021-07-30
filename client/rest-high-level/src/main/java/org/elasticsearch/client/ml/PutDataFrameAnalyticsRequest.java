/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class PutDataFrameAnalyticsRequest implements ToXContentObject, Validatable {

    private final DataFrameAnalyticsConfig config;

    public PutDataFrameAnalyticsRequest(DataFrameAnalyticsConfig config) {
        this.config = config;
    }

    public DataFrameAnalyticsConfig getConfig() {
        return config;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (config == null) {
            return Optional.of(ValidationException.withError("put requires a non-null data frame analytics config"));
        }
        return Optional.empty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return config.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PutDataFrameAnalyticsRequest other = (PutDataFrameAnalyticsRequest) o;
        return Objects.equals(config, other.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
