/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.service;

import org.elasticsearch.action.support.master.AcknowledgedResponse;

/**
 * Empty response, so if it returns, it means all is fine.
 */
public class WatcherServiceResponse extends AcknowledgedResponse {

    WatcherServiceResponse() {
    }

    public WatcherServiceResponse(boolean acknowledged) {
        super(acknowledged);
    }
}
