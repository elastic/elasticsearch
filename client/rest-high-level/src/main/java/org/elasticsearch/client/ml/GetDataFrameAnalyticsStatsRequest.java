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
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Request to get data frame analytics stats
 */
public class GetDataFrameAnalyticsStatsRequest implements Validatable {

    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

    private final List<String> ids;
    private Boolean allowNoMatch;
    private PageParams pageParams;

    public GetDataFrameAnalyticsStatsRequest(String... ids) {
        this.ids = Arrays.asList(ids);
    }

    public List<String> getIds() {
        return ids;
    }

    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    /**
     * Whether to ignore if a wildcard expression matches no data frame analytics.
     *
     * @param allowNoMatch If this is {@code false}, then an error is returned when a wildcard (or {@code _all})
     *                        does not match any data frame analytics
     */
    public GetDataFrameAnalyticsStatsRequest setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
        return this;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    public GetDataFrameAnalyticsStatsRequest setPageParams(@Nullable PageParams pageParams) {
        this.pageParams = pageParams;
        return this;
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

        GetDataFrameAnalyticsStatsRequest other = (GetDataFrameAnalyticsStatsRequest) o;
        return Objects.equals(ids, other.ids)
            && Objects.equals(allowNoMatch, other.allowNoMatch)
            && Objects.equals(pageParams, other.pageParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, allowNoMatch, pageParams);
    }
}
