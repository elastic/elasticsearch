/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition.compare.array;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.xpack.support.clock.Clock;
import org.elasticsearch.xpack.watcher.condition.compare.AbstractExecutableCompareCondition;
import org.elasticsearch.xpack.watcher.support.xcontent.ObjectPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExecutableArrayCompareCondition extends AbstractExecutableCompareCondition<ArrayCompareCondition,
        ArrayCompareCondition.Result> {

    public ExecutableArrayCompareCondition(ArrayCompareCondition condition, ESLogger logger, Clock clock) {
        super(condition, logger, clock);
    }

    public ArrayCompareCondition.Result doExecute(Map<String, Object> model, Map<String, Object> resolvedValues) {
        Object configuredValue = resolveConfiguredValue(resolvedValues, model, condition.getValue());

        Object object = ObjectPath.eval(condition.getArrayPath(), model);
        if (object != null && !(object instanceof List)) {
            throw new IllegalStateException("array path " + condition.getArrayPath() + " did not evaluate to array, was " + object);
        }

        @SuppressWarnings("unchecked")
        List<Object> resolvedArray = object != null ? (List<Object>) object : Collections.emptyList();

        List<Object> resolvedValue = new ArrayList<>(resolvedArray.size());
        for (int i = 0; i < resolvedArray.size(); i++) {
            resolvedValue.add(ObjectPath.eval(condition.getPath(), resolvedArray.get(i)));
        }
        resolvedValues.put(condition.getArrayPath(), resolvedArray);

        return new ArrayCompareCondition.Result(resolvedValues, condition.getQuantifier().eval(resolvedValue, configuredValue,
                condition.getOp()));
    }
}
