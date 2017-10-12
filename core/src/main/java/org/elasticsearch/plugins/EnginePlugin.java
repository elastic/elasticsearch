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

package org.elasticsearch.plugins;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;

import java.util.Optional;

/**
 * A plugin that provides alternative engine implementations.
 */
public interface EnginePlugin {

    /**
     * When an index is created this method is invoked for each engine plugin. Engine plugins can inspect the index settings to determine
     * whether or not to provide an engine factory for the given index. A plugin that is not overriding the default engine should return
     * {@link Optional#empty()}. If multiple plugins return an engine factory for a given index the index will not be created and an
     * {@link IllegalStateException} will be thrown during index creation.
     *
     * @return an optional engine factory
     */
    Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings);

}
