/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.client.internal.Client;

public class RankContext {

    protected final int size;
    protected final int from;
    protected final int windowSize;
    protected final Client client;

    public RankContext(int size, int from, int windowSize, Client client) {
        this.size = size;
        this.from = from;
        this.windowSize = windowSize;
        this.client = client;
    }

    public int getSize() {
        return size;
    }

    public int getFrom() {
        return from;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public Client getClient() {
        return client;
    }

}
