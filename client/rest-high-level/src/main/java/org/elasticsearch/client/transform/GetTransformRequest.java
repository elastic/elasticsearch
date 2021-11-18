/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    public static final String EXCLUDE_GENERATED = "exclude_generated";
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
    private Boolean excludeGenerated;

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

    public void setExcludeGenerated(boolean excludeGenerated) {
        this.excludeGenerated = excludeGenerated;
    }

    public Boolean getExcludeGenerated() {
        return excludeGenerated;
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
        return Objects.hash(ids, pageParams, excludeGenerated, allowNoMatch);
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
            && Objects.equals(excludeGenerated, other.excludeGenerated)
            && Objects.equals(allowNoMatch, other.allowNoMatch);
    }
}
