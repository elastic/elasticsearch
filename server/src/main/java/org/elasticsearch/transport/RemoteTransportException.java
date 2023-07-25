/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A remote exception for an action. A wrapper exception around the actual remote cause and does not fill the
 * stack trace. The skip_unavailable cluster setting of the remote cluster can optionally be set on the exception.
 */
public class RemoteTransportException extends ActionTransportException implements ElasticsearchWrapperException {

    // not Writeable, since it is only needed on a coordinator node for an active CCS search
    private final String clusterAlias;
    // not Writeable, since it is only needed on a coordinator node for an active CCS search
    private final Boolean skipUnavailable;

    public RemoteTransportException(String msg, Throwable cause) {
        super(msg, null, null, cause);
        this.clusterAlias = null;
        this.skipUnavailable = null;
    }

    /**
     * @param msg error message
     * @param cause underlying cause
     * @param clusterAlias cluster alias (from local cluster settings) for cluster with this Exception
     * @param skipUnavailable whether the remote cluster is marked with skip_unavailable in the cluster settings
     */
    public RemoteTransportException(String msg, Throwable cause, String clusterAlias, boolean skipUnavailable) {
        super(msg, null, null, cause);
        this.clusterAlias = clusterAlias;
        this.skipUnavailable = skipUnavailable;
    }

    public RemoteTransportException(String name, TransportAddress address, String action, Throwable cause) {
        super(name, address, action, cause);
        this.clusterAlias = null;
        this.skipUnavailable = null;
    }

    public RemoteTransportException(String name, InetSocketAddress address, String action, Throwable cause) {
        super(name, address, action, null, cause);
        this.clusterAlias = null;
        this.skipUnavailable = null;
    }

    public RemoteTransportException(StreamInput in) throws IOException {
        super(in);
        this.clusterAlias = null;
        this.skipUnavailable = null;
    }

    @Override
    public Throwable fillInStackTrace() {
        // no need for stack trace here, we always have cause
        return this;
    }

    /**
     * @return cluster alias of cluster with remote transport exception, if set.
     * If not set, returns null.
     */
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * @return If skipUnavailable flag was set on the Exception, returns that value.
     * Otherwise, returns null (unknown/not-set)
     */
    public Boolean getSkipUnavailable() {
        return skipUnavailable;
    }
}
