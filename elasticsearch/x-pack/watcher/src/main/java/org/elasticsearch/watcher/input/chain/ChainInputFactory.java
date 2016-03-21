/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.chain;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.InputFactory;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.xpack.common.init.LazyInitializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChainInputFactory extends InputFactory<ChainInput, ChainInput.Result, ExecutableChainInput>
        implements LazyInitializable {

    private InputRegistry inputRegistry;

    @Inject
    public ChainInputFactory(Settings settings) {
        super(Loggers.getLogger(ExecutableChainInput.class, settings));
    }

    @Override
    public String type() {
        return ChainInput.TYPE;
    }

    @Override
    public ChainInput parseInput(String watchId, XContentParser parser) throws IOException {
        return ChainInput.parse(watchId, parser, inputRegistry);
    }

    @Override
    public ExecutableChainInput createExecutable(ChainInput input) {
        List<Tuple<String, ExecutableInput>> executableInputs = new ArrayList<>();
        for (Tuple<String, Input> tuple : input.getInputs()) {
            ExecutableInput executableInput = inputRegistry.factories().get(tuple.v2().type()).createExecutable(tuple.v2());
            executableInputs.add(new Tuple<>(tuple.v1(), executableInput));
        }

        return new ExecutableChainInput(input, executableInputs, inputLogger);
    }

    @Override
    public void init(Injector injector) {
        init(injector.getInstance(InputRegistry.class));
    }

    void init(InputRegistry inputRegistry) {
        this.inputRegistry = inputRegistry;
    }
}
