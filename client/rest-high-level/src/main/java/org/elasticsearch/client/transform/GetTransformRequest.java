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

package org.elasticsearch.client.transform;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.core.PageParams;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GetTransformRequest implements Validatable {

    public static final String ALLOW_NO_MATCH = "allow_no_match";
    /**
     * Helper method to create a request that will get ALL Transforms
     * @return new {@link GetTransformRequest} object for the id "_all"
     */
    public static GetTransformRequest getAllTransformRequest() {
        return new GetTransformRequest("_all");
    }

    private final List<String> ids;
    private PageParams pageParams;
    private Boolean allowNoMatch;

    public GetTransformRequest(String... ids) {
        this.ids = Arrays.asList(ids);
    }

    public List<String> getId() {
        return ids;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    public void setAllowNoMatch(Boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (ids == null || ids.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("transform id must not be null");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, pageParams, allowNoMatch);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        GetTransformRequest other = (GetTransformRequest) obj;
        return Objects.equals(ids, other.ids)
            && Objects.equals(pageParams, other.pageParams)
            && Objects.equals(allowNoMatch, other.allowNoMatch);
    }
}
