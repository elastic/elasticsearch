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
package org.elasticsearch.client.rollup;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;

public class StopRollupJobRequest implements Validatable {

    private final String jobId;
    private TimeValue timeout;
    private Boolean waitForCompletion;

    public StopRollupJobRequest(final String jobId) {
        this.jobId = Objects.requireNonNull(jobId, "id parameter must not be null");
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StopRollupJobRequest that = (StopRollupJobRequest) o;
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId);
    }

    /**
     * Sets the requests optional "timeout" parameter.
     */
    public void timeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    /**
     * Sets the requests optional "wait_for_completion".
     */
    public void waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public Boolean waitForCompletion() {
        return this.waitForCompletion;
    }
}
