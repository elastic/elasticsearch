/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.batching.TransactionHandler;

import java.util.List;

class ShutdownTask<K> implements Task<K> {
    @Override
    public boolean shouldShutdown() {
        return true;
    }

    @Override
    public boolean hasFinished() {
        return true;
    }

    @Override
    public TransactionHandler<K> handler() {
        return null;
    }

    @Override
    public List<String> input() {
        return null;
    }

    @Override
    public ActionListener<HttpResult> listener() {
        return null;
    }

    @Override
    public void onRejection(Exception e) {

    }
}
