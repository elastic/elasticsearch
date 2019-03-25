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

package org.elasticsearch.client.dataframe;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GetDataFrameTransformRequest implements Validatable {

    private final List<String> ids;
    private Integer from;
    private Integer size;

    /**
     * Helper method to create a request that will get ALL Data Frame Transforms
     * @return new {@link GetDataFrameTransformRequest} object for the id "_all"
     */
    public static GetDataFrameTransformRequest getAllDataFrameTransformsRequest() {
        return new GetDataFrameTransformRequest("_all");
    }

    public GetDataFrameTransformRequest(String... ids) {
        this.ids = Arrays.asList(ids);
    }

    public List<String> getId() {
        return ids;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (ids == null || ids.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("data frame transform id must not be null");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        GetDataFrameTransformRequest other = (GetDataFrameTransformRequest) obj;
        return Objects.equals(ids, other.ids);
    }
}
