/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.none;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.InputFactory;

import java.io.IOException;

/**
 *
 */
public class NoneInputFactory extends InputFactory<NoneInput, NoneInput.Result, ExecutableNoneInput> {

    @Inject
    public NoneInputFactory(Settings settings) {
        super(Loggers.getLogger(ExecutableNoneInput.class, settings));
    }

    @Override
    public String type() {
        return NoneInput.TYPE;
    }

    @Override
    public NoneInput parseInput(String watchId, XContentParser parser) throws IOException {
        return NoneInput.parse(watchId, parser);
    }

    @Override
    public NoneInput.Result parseResult(String watchId, XContentParser parser) throws IOException {
        return NoneInput.Result.parse(watchId, parser);
    }

    @Override
    public ExecutableNoneInput createExecutable(NoneInput input) {
        return new ExecutableNoneInput(inputLogger);
    }
}
