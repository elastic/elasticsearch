/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.none;

import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.none.NoneInput;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

public class ExecutableNoneInput extends ExecutableInput<NoneInput, NoneInput.Result> {

    public ExecutableNoneInput() {
        super(NoneInput.INSTANCE);
    }

    @Override
    public NoneInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        return NoneInput.Result.INSTANCE;
    }

}
