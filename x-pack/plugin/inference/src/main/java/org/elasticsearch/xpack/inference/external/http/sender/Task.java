/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.xpack.inference.external.http.batching.BatchableRequest;

public interface Task<K, R> extends BatchableRequest<K, R> {
    boolean shouldShutdown();

    boolean hasFinished();

    void onRejection(Exception e);
}
