/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.compare.array;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.condition.ConditionFactory;
import org.elasticsearch.watcher.support.clock.Clock;

import java.io.IOException;

public class ArrayCompareConditionFactory extends ConditionFactory<ArrayCompareCondition, ArrayCompareCondition.Result,
        ExecutableArrayCompareCondition> {

    private final Clock clock;

    @Inject
    public ArrayCompareConditionFactory(Settings settings, Clock clock) {
        super(Loggers.getLogger(ExecutableArrayCompareCondition.class, settings));
        this.clock = clock;
    }

    @Override
    public String type() {
        return ArrayCompareCondition.TYPE;
    }

    @Override
    public ArrayCompareCondition parseCondition(String watchId, XContentParser parser) throws IOException {
        return ArrayCompareCondition.parse(watchId, parser);
    }

    @Override
    public ExecutableArrayCompareCondition createExecutable(ArrayCompareCondition condition) {
        return new ExecutableArrayCompareCondition(condition, conditionLogger, clock);
    }
}
