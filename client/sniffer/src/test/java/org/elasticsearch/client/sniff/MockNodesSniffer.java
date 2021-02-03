/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.sniff;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;

import java.util.Collections;
import java.util.List;

/**
 * Mock implementation of {@link NodesSniffer}. Useful to prevent any connection attempt while testing builders etc.
 */
class MockNodesSniffer implements NodesSniffer {
    @Override
    public List<Node> sniff() {
        return Collections.singletonList(new Node(new HttpHost("localhost", 9200)));
    }
}
