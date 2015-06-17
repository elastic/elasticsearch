/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.chain;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class ExecutableChainTransform extends ExecutableTransform<ChainTransform, ChainTransform.Result> {

    private final ImmutableList<ExecutableTransform> transforms;

    public ExecutableChainTransform(ChainTransform transform, ESLogger logger, ImmutableList<ExecutableTransform> transforms) {
        super(transform, logger);
        this.transforms = transforms;
    }

    public List<ExecutableTransform> executableTransforms() {
        return transforms;
    }

    @Override
    public ChainTransform.Result execute(WatchExecutionContext ctx, Payload payload) {
        ImmutableList.Builder<Transform.Result> results = ImmutableList.builder();
        try {
            return doExecute(ctx, payload, results);
        } catch (Exception e) {
            logger.error("failed to execute [{}] transform for [{}]", e, ChainTransform.TYPE, ctx.id());
            return new ChainTransform.Result(e, results.build());
        }
    }


    ChainTransform.Result doExecute(WatchExecutionContext ctx, Payload payload, ImmutableList.Builder<Transform.Result> results) throws IOException {
        for (ExecutableTransform transform : transforms) {
            Transform.Result result = transform.execute(ctx, payload);
            results.add(result);
            if (result.status() == Transform.Result.Status.FAILURE) {
                throw new ChainTransformException("failed to execute [{}] transform. failed to execute sub-transform [{}]", ChainTransform.TYPE, transform.type());
            }
            payload = result.payload();
        }
        return new ChainTransform.Result(payload, results.build());
    }

}
