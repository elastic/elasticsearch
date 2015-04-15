/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.always;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.condition.ConditionFactory;

import java.io.IOException;

/**
 *
 */
public class AlwaysConditionFactory extends ConditionFactory<AlwaysCondition, AlwaysCondition.Result, ExecutableAlwaysCondition> {

    private final ExecutableAlwaysCondition condition;

    @Inject
    public AlwaysConditionFactory(Settings settings) {
        super(Loggers.getLogger(ExecutableAlwaysCondition.class, settings));
        condition = new ExecutableAlwaysCondition(conditionLogger);
    }

    @Override
    public String type() {
        return AlwaysCondition.TYPE;
    }

    @Override
    public AlwaysCondition parseCondition(String watchId, XContentParser parser) throws IOException {
        return AlwaysCondition.parse(watchId, parser);
    }

    @Override
    public AlwaysCondition.Result parseResult(String watchId, XContentParser parser) throws IOException {
        return AlwaysCondition.Result.parse(watchId, parser);
    }

    @Override
    public ExecutableAlwaysCondition createExecutable(AlwaysCondition condition) {
        return this.condition;
    }
}
