/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.chain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.watcher.input.chain.ChainInput.TYPE;

public class ExecutableChainInput extends ExecutableInput<ChainInput, ChainInput.Result> {
    private static final Logger logger = LogManager.getLogger(ExecutableChainInput.class);

    private final List<Tuple<String, ExecutableInput<?, ?>>> inputs;

    public ExecutableChainInput(ChainInput input, List<Tuple<String, ExecutableInput<?, ?>>> inputs) {
        super(input);
        this.inputs = inputs;
    }

    @Override
    public ChainInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        List<Tuple<String, Input.Result>> results = new ArrayList<>();
        Map<String, Object> payloads = new HashMap<>();

        try {
            for (Tuple<String, ExecutableInput<?, ?>> tuple : inputs) {
                Input.Result result = tuple.v2().execute(ctx, new Payload.Simple(payloads));
                results.add(new Tuple<>(tuple.v1(), result));
                payloads.put(tuple.v1(), result.payload().data());
            }

            return new ChainInput.Result(results, new Payload.Simple(payloads));
        } catch (Exception e) {
            logger.error(() -> format("failed to execute [%s] input for watch [%s]", TYPE, ctx.watch().id()), e);
            return new ChainInput.Result(e);
        }
    }
}
