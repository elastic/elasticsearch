/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.chain;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformFactory;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.common.init.LazyInitializable;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class ChainTransformFactory extends TransformFactory<ChainTransform, ChainTransform.Result, ExecutableChainTransform> implements
        LazyInitializable {

    private TransformRegistry registry;

    // used by guice
    public ChainTransformFactory(Settings settings) {
        super(Loggers.getLogger(ExecutableChainTransform.class, settings));
    }

    // used for tests
    public ChainTransformFactory(TransformRegistry registry) {
        super(Loggers.getLogger(ExecutableChainTransform.class));
        this.registry = registry;
    }

    // used for tests
    public ChainTransformFactory() {
        super(Loggers.getLogger(ExecutableChainTransform.class));
    }

    @Override
    public void init(Injector injector) {
        init(injector.getInstance(TransformRegistry.class));
    }

    public void init(TransformRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String type() {
        return ChainTransform.TYPE;
    }

    @Override
    public ChainTransform parseTransform(String watchId, XContentParser parser) throws IOException {
        return ChainTransform.parse(watchId, parser, registry);
    }

    @Override
    public ExecutableChainTransform createExecutable(ChainTransform chainTransform) {
        ArrayList<ExecutableTransform> executables = new ArrayList<>();
        for (Transform transform : chainTransform.getTransforms()) {
            TransformFactory factory = registry.factory(transform.type());
            executables.add(factory.createExecutable(transform));
        }
        return new ExecutableChainTransform(chainTransform, transformLogger, executables);
    }
}
