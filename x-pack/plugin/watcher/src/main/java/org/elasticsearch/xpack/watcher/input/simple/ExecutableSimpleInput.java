/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.simple;

import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

/**
 * This class just defines a simple xcontent map as an input
 */
public class ExecutableSimpleInput extends ExecutableInput<SimpleInput, SimpleInput.Result> {

    public ExecutableSimpleInput(SimpleInput input) {
        super(input);
    }

    @Override
    public SimpleInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        return new SimpleInput.Result(input.getPayload());
    }
}
