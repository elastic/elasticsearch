/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.index.shard.ExplicitIndexSettingProvider;

import java.util.Collection;
import java.util.Collections;

/**
 * An {@link IndexSettingsProviderPlugin} is a plugin that allows hooking in to parts of an index
 * lifecycle to provide explicit default settings for newly created indices. Rather than changing
 * the default values for an index-level setting, these act as though the setting has been set
 * explicitly, but still allow the setting to be overridden by a template or creation request body.
 */
public interface IndexSettingsProviderPlugin {
    default Collection<ExplicitIndexSettingProvider> getExplicitSettingProviders() {
        return Collections.emptyList();
    }
}
