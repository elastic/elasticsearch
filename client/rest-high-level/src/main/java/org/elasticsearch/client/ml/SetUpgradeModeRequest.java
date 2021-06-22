/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

/**
 * Sets ML into upgrade_mode
 */
public class SetUpgradeModeRequest implements Validatable {


    public static final ParseField ENABLED = new ParseField("enabled");
    public static final ParseField TIMEOUT = new ParseField("timeout");

    private boolean enabled;
    private TimeValue timeout;

    /**
     * Create a new request
     *
     * @param enabled whether to enable `upgrade_mode` or not
     */
    public SetUpgradeModeRequest(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * How long to wait for the request to be completed
     *
     * @param timeout default value of 30 seconds
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, timeout);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        SetUpgradeModeRequest that = (SetUpgradeModeRequest) other;
        return Objects.equals(enabled, that.enabled) && Objects.equals(timeout, that.timeout);
    }
}
