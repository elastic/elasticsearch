/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.always;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.execution.WatchExecutionContext;

import java.io.IOException;

/**
 */
public class ExecutableAlwaysCondition extends ExecutableCondition<AlwaysCondition, AlwaysCondition.Result> {

    public ExecutableAlwaysCondition(ESLogger logger) {
        super(AlwaysCondition.INSTANCE, logger);
    }

    @Override
    public AlwaysCondition.Result execute(WatchExecutionContext ctx) throws IOException {
        return AlwaysCondition.Result.INSTANCE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ExecutableAlwaysCondition;
    }

}
