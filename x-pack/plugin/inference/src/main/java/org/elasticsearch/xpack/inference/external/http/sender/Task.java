/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.batching.Handler;

import java.util.List;

public interface Task<K, R> {
    boolean shouldShutdown();

    boolean hasFinished();

    Handler<K, R> handler();

    List<String> input();

    ActionListener<HttpResult> listener();

    void onRejection(Exception e);
}
