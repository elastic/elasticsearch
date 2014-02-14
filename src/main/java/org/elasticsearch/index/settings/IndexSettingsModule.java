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

package org.elasticsearch.index.settings;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

/**
 *
 */
public class IndexSettingsModule extends AbstractModule {

    private final Index index;

    private final Settings settings;

    public IndexSettingsModule(Index index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    @Override
    protected void configure() {
        IndexSettingsService indexSettingsService = new IndexSettingsService(index, settings);
        bind(IndexSettingsService.class).toInstance(indexSettingsService);
        bind(Settings.class).annotatedWith(IndexSettings.class).toProvider(new IndexSettingsProvider(indexSettingsService));
    }
}
