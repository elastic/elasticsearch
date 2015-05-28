/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.never;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.condition.ConditionFactory;

import java.io.IOException;

/**
 *
 */
public class NeverConditionFactory extends ConditionFactory<NeverCondition, NeverCondition.Result, ExecutableNeverCondition> {

    private final ExecutableNeverCondition condition;

    @Inject
    public NeverConditionFactory(Settings settings) {
        super(Loggers.getLogger(ExecutableNeverCondition.class, settings));
        condition = new ExecutableNeverCondition(conditionLogger);
    }

    @Override
    public String type() {
        return NeverCondition.TYPE;
    }

    @Override
    public NeverCondition parseCondition(String watchId, XContentParser parser) throws IOException {
        return NeverCondition.parse(watchId, parser);
    }

    @Override
    public ExecutableNeverCondition createExecutable(NeverCondition condition) {
        return this.condition;
    }
}
