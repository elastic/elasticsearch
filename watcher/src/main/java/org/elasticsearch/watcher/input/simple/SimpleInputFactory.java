/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.simple;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.InputFactory;

import java.io.IOException;

/**
 *
 */
public class SimpleInputFactory extends InputFactory<SimpleInput, SimpleInput.Result, ExecutableSimpleInput> {

    @Inject
    public SimpleInputFactory(Settings settings) {
        super(Loggers.getLogger(ExecutableSimpleInput.class, settings));
    }

    @Override
    public String type() {
        return SimpleInput.TYPE;
    }

    @Override
    public SimpleInput parseInput(String watchId, XContentParser parser) throws IOException {
        return SimpleInput.parse(watchId, parser);
    }

    @Override
    public ExecutableSimpleInput createExecutable(SimpleInput input) {
        return new ExecutableSimpleInput(input, inputLogger);
    }
}
