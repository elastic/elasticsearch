/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parses xcontent to a concrete condition of the same type.
 */
public abstract class ConditionFactory<C extends Condition, R extends Condition.Result, E extends ExecutableCondition<C, R>> {

    protected final Logger conditionLogger;

    public ConditionFactory(Logger conditionLogger) {
        this.conditionLogger = conditionLogger;
    }

    /**
     * @return  The type of the condition
     */
    public abstract String type();

    /**
     * Parses the given xcontent and creates a concrete condition
     */
    public abstract C parseCondition(String watchId, XContentParser parser) throws IOException;

    /**
     * Creates an {@link ExecutableCondition executable condition} for the given condition.
     */
    public abstract E createExecutable(C condition);
}
