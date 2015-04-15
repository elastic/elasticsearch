/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.never;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.execution.WatchExecutionContext;

import java.io.IOException;

/**
 */
public class ExecutableNeverCondition extends ExecutableCondition<NeverCondition, NeverCondition.Result> {

    public ExecutableNeverCondition(ESLogger logger) {
        super(NeverCondition.INSTANCE, logger);
    }

    @Override
    public NeverCondition.Result execute(WatchExecutionContext ctx) throws IOException {
        return NeverCondition.Result.INSTANCE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ExecutableNeverCondition;
    }

}
