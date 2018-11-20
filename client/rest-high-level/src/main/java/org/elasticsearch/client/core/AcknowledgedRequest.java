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
package org.elasticsearch.client.core;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.unit.TimeValue;

/**
 * A rest request that is acknowledged by a master
 */
public class AcknowledgedRequest implements Validatable {

    private TimeValue masterNodeTimeout;
    private TimeValue timeout;

    /**
     * Allows to set the timeout
     * @param timeout timeout as a {@link TimeValue}
     */
    public final void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    public final void setMasterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
    }

    /**
     * The master timeout or null if the server default should be used.
     */
    public TimeValue getMasterNodeTimeout() {
        return masterNodeTimeout;
    }

    /**
     * The timeout or null if the server default should be used.
     */
    public TimeValue getTimeout() {
        return timeout;
    }
}
