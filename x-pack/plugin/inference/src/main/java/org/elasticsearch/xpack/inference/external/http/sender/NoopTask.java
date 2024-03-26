/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;

import java.util.List;
import java.util.function.Supplier;

class NoopTask implements RejectableTask {

    @Override
    public ExecutableRequestCreator getRequestCreator() {
        return null;
    }

    @Override
    public List<String> getInput() {
        return null;
    }

    @Override
    public ActionListener<InferenceServiceResults> getListener() {
        return null;
    }

    @Override
    public boolean hasCompleted() {
        return true;
    }

    @Override
    public Supplier<Boolean> getRequestCompletedFunction() {
        return () -> true;
    }

    @Override
    public void onRejection(Exception e) {

    }
}
