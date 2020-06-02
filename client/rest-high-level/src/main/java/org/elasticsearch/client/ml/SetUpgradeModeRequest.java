/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;

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
