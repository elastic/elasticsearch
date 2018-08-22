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

package org.elasticsearch.client;

import org.elasticsearch.common.unit.TimeValue;

// TODO: This class exists as a stopgap to allow progress in the new HLRC structure.
// It should be replaced with a version of MasterNodeRequest that will likely be
// created when everything is moved from o.e.xpack.protocol to o.e.client.
public abstract class MasterTimeoutRequest<Request> implements Validatable {

    protected TimeValue timeout;

    @SuppressWarnings("unchecked")
    public Request masterNodeTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    public final Request masterNodeTimeout(String timeout) {
        return masterNodeTimeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".masterNodeTimeout"));
    }

    public TimeValue masterNodeTimeout() {
        return timeout;
    }
}
