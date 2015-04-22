/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.chain;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class ExecutableChainTransform extends ExecutableTransform<ChainTransform, ChainTransform.Result> {

    private final ImmutableList<ExecutableTransform> transforms;

    public ExecutableChainTransform(ChainTransform transform, ESLogger logger, ImmutableList<ExecutableTransform> transforms) {
        super(transform, logger);
        this.transforms = transforms;
    }

    public ImmutableList<ExecutableTransform> executableTransforms() {
        return transforms;
    }

    @Override
    public ChainTransform.Result execute(WatchExecutionContext ctx, Payload payload) throws IOException {
        ImmutableList.Builder<Transform.Result> results = ImmutableList.builder();
        for (ExecutableTransform transform : transforms) {
            Transform.Result result = transform.execute(ctx, payload);
            results.add(result);
            payload = result.payload();
        }
        return new ChainTransform.Result(payload, results.build());
    }

}
