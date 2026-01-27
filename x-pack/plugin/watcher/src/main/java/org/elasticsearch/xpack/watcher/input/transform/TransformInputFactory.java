/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.transform;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.transform.TransformFactory;
import org.elasticsearch.xpack.core.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.watcher.input.InputFactory;

import java.io.IOException;

/**
 *
 * Transform inputs should be used between two other inputs in a chained input,
 * so that you can do a transformation of your data, before sending it off to
 * another input
 *
 * The transform input factory is pretty lightweight, as all the infra structure
 * for transform can be reused for this
 *
 */
public final class TransformInputFactory extends InputFactory<TransformInput, TransformInput.Result, ExecutableTransformInput> {

    private final TransformRegistry transformRegistry;

    public TransformInputFactory(TransformRegistry transformRegistry) {
        this.transformRegistry = transformRegistry;
    }

    @Override
    public String type() {
        return TransformInput.TYPE;
    }

    @Override
    public TransformInput parseInput(String watchId, XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Transform transform = transformRegistry.parse(watchId, parser).transform();
        return new TransformInput(transform);
    }

    @Override
    public ExecutableTransformInput createExecutable(TransformInput input) {
        Transform transform = input.getTransform();
        @SuppressWarnings("unchecked")
        TransformFactory<Transform, ?, ?> factory = (TransformFactory<Transform, ?, ?>) transformRegistry.factory(transform.type());
        ExecutableTransform<?, ?> executableTransform = factory.createExecutable(transform);
        return new ExecutableTransformInput(input, executableTransform);
    }
}
