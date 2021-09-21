/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.transport.TransportAddress;

public interface TransportAddressConnector {
    /**
     * Identify the node at the given address and, if it is a master node and not the local node then establish a full connection to it.
     */
    void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<ProbeConnectionResult> listener);
}
