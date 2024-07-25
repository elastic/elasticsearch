/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * A based request for master based operation.
 */
public abstract class MasterNodeRequest<Request extends MasterNodeRequest<Request>> extends ActionRequest {

    /**
     * The default timeout for master-node requests. It's super-trappy to have such a default, because it makes it all too easy to forget
     * to add a mechanism by which clients can change it. Without such a mechanism things will work fine until we encounter a large cluster
     * that is struggling to process cluster state updates fast enough, and it's a disaster if we cannot extend the master-node timeout in
     * those cases. We shouldn't use this any more and should work towards removing it.
     * <p>
     * For requests which originate in the REST layer, use {@link org.elasticsearch.rest.RestUtils#getMasterNodeTimeout} to determine the
     * timeout.
     * <p>
     * For internally-generated requests, choose an appropriate timeout. Often this will be {@link TimeValue#MAX_VALUE} (or {@link
     * TimeValue#MINUS_ONE} which means an infinite timeout in 8.15.0 onwards) since usually we want internal requests to wait for as long
     * as necessary to complete.
     *
     * @deprecated all requests should specify a timeout, see <a href="https://github.com/elastic/elasticsearch/issues/107984">#107984</a>.
     */
    @Deprecated(forRemoval = true)
    public static final TimeValue TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private TimeValue masterNodeTimeout;

    /**
     * The term of the cluster state version used to route this request to a different node, so that if two nodes disagree about which of
     * them is the master then they don't just send these requests in a loop. {@code 0L} means this is the original request that hasn't been
     * rerouted yet, or else it's a request received from an older version which doesn't have the routing loop protection.
     */
    private final long masterTerm;

    /**
     * @param masterNodeTimeout Specifies how long to wait when the master has not been discovered yet, or is disconnected, or is busy
     *                          processing other tasks. The value {@link TimeValue#MINUS_ONE} means to wait forever in 8.15.0 onwards.
     *                          <p>
     *                          For requests which originate in the REST layer, use {@link
     *                          org.elasticsearch.rest.RestUtils#getMasterNodeTimeout} to determine the timeout.
     *                          <p>
     *                          For internally-generated requests, choose an appropriate timeout. Often this will be {@link
     *                          TimeValue#MAX_VALUE} (or {@link TimeValue#MINUS_ONE} which means an infinite timeout in 8.15.0 onwards)
     *                          since usually we want internal requests to wait for as long as necessary to complete.
     */
    protected MasterNodeRequest(TimeValue masterNodeTimeout) {
        this.masterNodeTimeout = Objects.requireNonNull(masterNodeTimeout);
        this.masterTerm = 0L;
    }

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
        masterNodeTimeout = in.readTimeValue();
        if (in.getTransportVersion().onOrAfter(TransportVersions.VERSIONED_MASTER_NODE_REQUESTS)) {
            masterTerm = in.readVLong();
        } else {
            masterTerm = 0L;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final var newMasterTerm = getNewMasterTerm(out);
        assert hasReferences();
        assert masterTerm <= newMasterTerm : masterTerm + " vs " + newMasterTerm;
        super.writeTo(out);
        out.writeTimeValue(masterNodeTimeout);
        if (out.getTransportVersion().onOrAfter(TransportVersions.VERSIONED_MASTER_NODE_REQUESTS)) {
            out.writeVLong(newMasterTerm);
        } // else no protection against routing loops in older versions
    }

    private long getNewMasterTerm(StreamOutput out) {
        if (out instanceof TermOverridingStreamOutput termOverridingStreamOutput) {
            return termOverridingStreamOutput.masterTerm;
        } else {
            return masterTerm;
        }
    }

    /**
     * Specifies how long to wait when the master has not been discovered yet, or is disconnected, or is busy processing other tasks. The
     * value {@link TimeValue#MINUS_ONE} means to wait forever in 8.15.0 onwards.
     * <p>
     * For requests which originate in the REST layer, use {@link org.elasticsearch.rest.RestUtils#getMasterNodeTimeout} to determine the
     * timeout.
     * <p>
     * For internally-generated requests, choose an appropriate timeout. Often this will be {@link TimeValue#MAX_VALUE} (or {@link
     * TimeValue#MINUS_ONE} which means an infinite timeout in 8.15.0 onwards) since usually we want internal requests to wait for as long
     * as necessary to complete.
     */
    @SuppressWarnings("unchecked")
    public final Request masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = Objects.requireNonNull(timeout);
        return (Request) this;
    }

    /**
     * @return how long to wait when the master has not been discovered yet, or is disconnected, or is busy processing other tasks. The
     * value {@link TimeValue#MINUS_ONE} means to wait forever.
     */
    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout;
    }

    /**
     * @return the master term of the cluster state used to route this request, for protection against routing loops. May be {@code 0L} if
     * this is the original request or it came from an older node that lacks routing loop protection
     */
    public final long masterTerm() {
        return masterTerm;
    }
}
