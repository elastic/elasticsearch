/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.compare;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.xcontent.ObjectPath;

import java.util.Map;


/**
 *
 */
public class ExecutableCompareCondition extends AbstractExecutableCompareCondition<CompareCondition, CompareCondition.Result> {
    public ExecutableCompareCondition(CompareCondition condition, ESLogger logger, Clock clock) {
        super(condition, logger, clock);
    }

    @Override
    protected CompareCondition.Result doExecute(Map<String, Object> model, Map<String, Object> resolvedValues) throws Exception {
        Object configuredValue = resolveConfiguredValue(resolvedValues, model, condition.getValue());

        Object resolvedValue = ObjectPath.eval(condition.getPath(), model);
        resolvedValues.put(condition.getPath(), resolvedValue);

        return new CompareCondition.Result(resolvedValues, condition.getOp().eval(resolvedValue, configuredValue));
    }

    @Override
    protected CompareCondition.Result doFailure(Map<String, Object> resolvedValues, Exception e) {
        return new CompareCondition.Result(resolvedValues, e);
    }
}
