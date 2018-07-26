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
import org.elasticsearch.index.store.IndexStore;

import java.util.Map;
import java.util.function.Function;

/**
 * A plugin that provides alternative index store implementations.
 */
public interface IndexStorePlugin {

    /**
     * The index store factories for this plugin. When an index is created the store type setting
     * {@link org.elasticsearch.index.IndexModule#INDEX_STORE_TYPE_SETTING} on the index will be examined and either use the default or a
     * built-in type, or looked up among all the index store factories from {@link IndexStore} plugins.
     *
     * @return a map from store type to an index store factory
     */
    Map<String, Function<IndexSettings, IndexStore>> getIndexStoreFactories();

}
