/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.simple;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.watch.Payload;

/**
 * This class just defines a simple xcontent map as an input
 */
public class ExecutableSimpleInput extends ExecutableInput<SimpleInput, SimpleInput.Result> {

    public ExecutableSimpleInput(SimpleInput input, ESLogger logger) {
        super(input, logger);
    }

    @Override
    public SimpleInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        return new SimpleInput.Result(input.getPayload());
    }
}
