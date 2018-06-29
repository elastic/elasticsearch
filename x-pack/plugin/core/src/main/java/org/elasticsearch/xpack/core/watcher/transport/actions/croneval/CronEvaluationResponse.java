/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.croneval;

import org.elasticsearch.action.ActionResponse;

import java.util.List;

/*
 * No serialization because the response is always answered locally and never sent over the network
 */
public class CronEvaluationResponse extends ActionResponse {

    private List<String> timestamps;

    public CronEvaluationResponse() {}

    public CronEvaluationResponse(List<String> timestamps) {
        this.timestamps = timestamps;
    }

    public List<String> getTimestamps() {
        return timestamps;
    }
}
