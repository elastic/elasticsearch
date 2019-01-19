/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.none;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.input.none.NoneInput;
import org.elasticsearch.xpack.watcher.input.InputFactory;

import java.io.IOException;

public class NoneInputFactory extends InputFactory<NoneInput, NoneInput.Result, ExecutableNoneInput> {
    @Override
    public String type() {
        return NoneInput.TYPE;
    }

    @Override
    public NoneInput parseInput(String watchId, XContentParser parser) throws IOException {
        return NoneInput.parse(watchId, parser);
    }

    @Override
    public ExecutableNoneInput createExecutable(NoneInput input) {
        return new ExecutableNoneInput();
    }
}
