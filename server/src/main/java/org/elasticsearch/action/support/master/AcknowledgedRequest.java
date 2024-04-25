/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;

/**
 * Abstract base class for action requests that track acknowledgements of cluster state updates: such a request is acknowledged only once
 * the cluster state update is committed and all relevant nodes have applied it and acknowledged its application to the elected master..
 */
public abstract class AcknowledgedRequest<Request extends MasterNodeRequest<Request>> extends MasterNodeRequest<Request>
    implements
        AckedRequest {

    public static final TimeValue DEFAULT_ACK_TIMEOUT = timeValueSeconds(30);

    /**
     * Specifies how long to wait for all relevant nodes to apply a cluster state update and acknowledge this to the elected master.
     */
    private TimeValue ackTimeout;

    /**
     * Construct an {@link AcknowledgedRequest} with the default ack timeout of 30s.
     */
    protected AcknowledgedRequest() {
        this(DEFAULT_ACK_TIMEOUT);
    }

    /**
     * @param ackTimeout specifies how long to wait for all relevant nodes to apply a cluster state update and acknowledge this to the
     *                   elected master.
     */
    protected AcknowledgedRequest(TimeValue ackTimeout) {
        this.ackTimeout = Objects.requireNonNull(ackTimeout);
    }

    protected AcknowledgedRequest(StreamInput in) throws IOException {
        super(in);
        this.ackTimeout = in.readTimeValue();
    }

    /**
     * Sets the {@link #ackTimeout}, which specifies how long to wait for all relevant nodes to apply a cluster state update and acknowledge
     * this to the elected master.
     *
     * @param ackTimeout timeout as a {@link TimeValue}
     * @return this request, for method chaining.
     */
    @SuppressWarnings("unchecked")
    public final Request ackTimeout(TimeValue ackTimeout) {
        this.ackTimeout = Objects.requireNonNull(ackTimeout);
        return (Request) this;
    }

    /**
     * @return the current ack timeout as a {@link TimeValue}
     */
    @Override
    public final TimeValue ackTimeout() {
        return ackTimeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(ackTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * {@link AcknowledgedRequest} that does not have any additional fields. Should be used instead of implementing noop subclasses of
     * {@link AcknowledgedRequest}.
     */
    public static final class Plain extends AcknowledgedRequest<Plain> {
        public Plain(StreamInput in) throws IOException {
            super(in);
        }

        public Plain() {}
    }
}
