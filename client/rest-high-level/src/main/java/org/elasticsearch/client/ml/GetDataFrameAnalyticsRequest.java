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
import org.elasticsearch.common.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GetDataFrameAnalyticsRequest implements Validatable {

    private final List<String> ids;
    private Integer from;
    private Integer size;

    /**
     * Helper method to create a request that will get ALL Data Frame Analytics
     * @return new {@link GetDataFrameAnalyticsRequest} object for the id "_all"
     */
    public static GetDataFrameAnalyticsRequest getAllDataFrameAnalyticsRequest() {
        return new GetDataFrameAnalyticsRequest("_all");
    }

    public GetDataFrameAnalyticsRequest(String... ids) {
        this.ids = Arrays.asList(ids);
    }

    public List<String> getIds() {
        return ids;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(@Nullable Integer from) {
        this.from = from;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(@Nullable Integer size) {
        this.size = size;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (ids == null || ids.isEmpty()) {
            return Optional.of(ValidationException.withError("data frame analytics id must not be null"));
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetDataFrameAnalyticsRequest other = (GetDataFrameAnalyticsRequest) o;
        return Objects.equals(ids, other.ids)
            && Objects.equals(from, other.from)
            && Objects.equals(size, other.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, from, size);
    }
}
