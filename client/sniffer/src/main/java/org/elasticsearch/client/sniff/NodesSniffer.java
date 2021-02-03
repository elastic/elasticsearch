/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.sniff;

import org.elasticsearch.client.Node;

import java.io.IOException;
import java.util.List;

/**
 * Responsible for sniffing the http hosts
 */
public interface NodesSniffer {
    /**
     * Returns the sniffed Elasticsearch nodes.
     */
    List<Node> sniff() throws IOException;
}
