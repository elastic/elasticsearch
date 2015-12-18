/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.chain;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.watch.Payload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutableChainInput extends ExecutableInput<ChainInput,ChainInput.Result> {

    private List<Tuple<String, ExecutableInput>> inputs;

    public ExecutableChainInput(ChainInput input, List<Tuple<String, ExecutableInput>> inputs, ESLogger logger) {
        super(input, logger);
        this.inputs = inputs;
    }

    @Override
    public ChainInput.Result execute(WatchExecutionContext ctx, Payload payload) {
        List<Tuple<String, Input.Result>> results = new ArrayList<>();
        Map<String, Object> payloads = new HashMap<>();

        try {
            for (Tuple<String, ExecutableInput> tuple : inputs) {
                Input.Result result = tuple.v2().execute(ctx, new Payload.Simple(payloads));
                results.add(new Tuple<>(tuple.v1(), result));
                payloads.put(tuple.v1(), result.payload().data());
            }

            return new ChainInput.Result(results, new Payload.Simple(payloads));
        } catch (Exception e) {
            logger.error("failed to execute [{}] input for [{}]", e, ChainInput.TYPE, ctx.watch());
            return new ChainInput.Result(e);
        }
    }
}
