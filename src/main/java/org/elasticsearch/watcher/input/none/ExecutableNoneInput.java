/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.none;


import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.ExecutableInput;

import java.io.IOException;

/**
 *
 */
public class ExecutableNoneInput extends ExecutableInput<NoneInput, NoneInput.Result> {

    public ExecutableNoneInput(ESLogger logger) {
        super(NoneInput.INSTANCE, logger);
    }

    @Override
    public NoneInput.Result execute(WatchExecutionContext ctx) throws IOException {
        return NoneInput.Result.INSTANCE;
    }

}
