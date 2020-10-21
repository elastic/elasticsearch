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
