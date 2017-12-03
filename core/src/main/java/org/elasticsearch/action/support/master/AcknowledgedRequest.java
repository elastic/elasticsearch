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
package org.elasticsearch.action.support.master;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * Abstract class that allows to mark action requests that support acknowledgements.
 * Facilitates consistency across different api.
 */
public abstract class AcknowledgedRequest<Request extends MasterNodeRequest<Request>> extends MasterNodeRequest<Request> implements AckedRequest {

    public static final TimeValue DEFAULT_ACK_TIMEOUT = timeValueSeconds(30);

    protected TimeValue timeout = DEFAULT_ACK_TIMEOUT;

    protected AcknowledgedRequest() {
    }

    /**
     * Allows to set the timeout
     * @param timeout timeout as a string (e.g. 1s)
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public final Request timeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, this.timeout, getClass().getSimpleName() + ".timeout");
        return (Request)this;
    }

    /**
     * Allows to set the timeout
     * @param timeout timeout as a {@link TimeValue}
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    /**
     * Returns the current timeout
     * @return the current timeout as a {@link TimeValue}
     */
    public final TimeValue timeout() {
        return  timeout;
    }

    @Override
    public TimeValue ackTimeout() {
        return timeout;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        timeout = new TimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        timeout.writeTo(out);
    }
}
