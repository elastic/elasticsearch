/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportRequest;

public interface HttpRemoteClusterService {

    boolean isHttpRemoteClusterAlias(String clusterAlias);

    void relayRequest(String clusterAlias, String action, TransportRequest transportRequest, ActionListener<byte[]> listener);
}
