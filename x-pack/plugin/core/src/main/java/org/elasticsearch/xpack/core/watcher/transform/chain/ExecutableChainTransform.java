/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transform.chain;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.core.watcher.transform.chain.ChainTransform.TYPE;

@SuppressWarnings("rawtypes")
public class ExecutableChainTransform extends ExecutableTransform<ChainTransform, ChainTransform.Result> {

    private final List<ExecutableTransform> transforms;

    public ExecutableChainTransform(ChainTransform transform, Logger logger, ExecutableTransform... transforms) {
        this(transform, logger, Arrays.asList(transforms));
    }

    public ExecutableChainTransform(ChainTransform transform, Logger logger, List<ExecutableTransform> transforms) {
        super(transform, logger);
        this.transforms = Collections.unmodifiableList(transforms);
    }

    public List<ExecutableTransform> executableTransforms() {
        return transforms;
    }

    @Override
    public ChainTransform.Result execute(WatchExecutionContext ctx, Payload payload) {
        List<Transform.Result> results = new ArrayList<>();
        try {
            return doExecute(ctx, payload, results);
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to execute [{}] transform for [{}]", TYPE, ctx.id()), e);
            return new ChainTransform.Result(e, results);
        }
    }

    ChainTransform.Result doExecute(WatchExecutionContext ctx, Payload payload, List<Transform.Result> results) throws IOException {
        for (ExecutableTransform transform : transforms) {
            Transform.Result result = transform.execute(ctx, payload);
            results.add(result);
            if (result.status() == Transform.Result.Status.FAILURE) {
                return new ChainTransform.Result(
                    format(
                        "failed to execute [{}] transform for [{}]. failed to execute sub-transform [{}]",
                        ChainTransform.TYPE,
                        ctx.id(),
                        transform.type()
                    ),
                    results
                );
            }
            payload = result.payload();
        }
        return new ChainTransform.Result(payload, results);
    }

}
