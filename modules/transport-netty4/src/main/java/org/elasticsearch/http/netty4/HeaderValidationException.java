/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.elasticsearch.ElasticsearchException;

public class HeaderValidationException extends Exception {
    private final boolean closeChannel;

    public HeaderValidationException(ElasticsearchException cause, boolean closeChannel) {
        super(cause);
        this.closeChannel = closeChannel;
    }

    public boolean shouldCloseChannel() {
        return closeChannel;
    }
}
