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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.Optional;

public class StopDataFrameAnalyticsRequest implements Validatable {

    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");
    public static final ParseField FORCE = new ParseField("force");

    private final String id;
    private Boolean allowNoMatch;
    private Boolean force;
    private TimeValue timeout;

    public StopDataFrameAnalyticsRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public StopDataFrameAnalyticsRequest setTimeout(@Nullable TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    public StopDataFrameAnalyticsRequest setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
        return this;
    }

    public Boolean getForce() {
        return force;
    }

    public StopDataFrameAnalyticsRequest setForce(boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (id == null) {
            return Optional.of(ValidationException.withError("data frame analytics id must not be null"));
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StopDataFrameAnalyticsRequest other = (StopDataFrameAnalyticsRequest) o;
        return Objects.equals(id, other.id)
            && Objects.equals(timeout, other.timeout)
            && Objects.equals(allowNoMatch, other.allowNoMatch)
            && Objects.equals(force, other.force);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timeout, allowNoMatch, force);
    }
}
