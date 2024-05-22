/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

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
    }

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
        masterNodeTimeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        assert hasReferences();
        out.writeTimeValue(masterNodeTimeout);
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
}
