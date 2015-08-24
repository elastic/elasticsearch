/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.engine;

import org.apache.lucene.index.FilterDirectoryReader;
import org.elasticsearch.common.inject.BindingAnnotation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class MockEngineFactory implements EngineFactory {
    @BindingAnnotation
    @Target({FIELD, PARAMETER})
    @Retention(RUNTIME)
    public @interface MockReaderType {
    }

    private Class<? extends FilterDirectoryReader> wrapper;

    @Inject
    public MockEngineFactory(@MockReaderType Class wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config, boolean skipTranslogRecovery) {
        return new MockInternalEngine(config, skipTranslogRecovery, wrapper);
    }

    @Override
    public Engine newReadOnlyEngine(EngineConfig config) {
        return new MockShadowEngine(config, wrapper);
    }
}
