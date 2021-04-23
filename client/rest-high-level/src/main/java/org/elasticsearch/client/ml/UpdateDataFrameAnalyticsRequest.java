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
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class UpdateDataFrameAnalyticsRequest implements ToXContentObject, Validatable {

    private final DataFrameAnalyticsConfigUpdate update;

    public UpdateDataFrameAnalyticsRequest(DataFrameAnalyticsConfigUpdate update) {
        this.update = update;
    }

    public DataFrameAnalyticsConfigUpdate getUpdate() {
        return update;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (update == null) {
            return Optional.of(ValidationException.withError("update requires a non-null data frame analytics config update"));
        }
        return Optional.empty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return update.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateDataFrameAnalyticsRequest other = (UpdateDataFrameAnalyticsRequest) o;
        return Objects.equals(update, other.update);
    }

    @Override
    public int hashCode() {
        return Objects.hash(update);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
