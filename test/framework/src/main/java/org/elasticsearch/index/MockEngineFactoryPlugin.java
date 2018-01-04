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
package org.elasticsearch.index;

import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.engine.MockEngineFactory;
import org.elasticsearch.test.engine.MockEngineSupport;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A plugin to use {@link MockEngineFactory}.
 *
 * Subclasses may override the reader wrapper used.
 */
public class MockEngineFactoryPlugin extends Plugin {

    public static final String EXCEPTION_TOP_LEVEL_RATIO_KEY = "index.engine.exception.ratio.top";
    public static final String EXCEPTION_LOW_LEVEL_RATIO_KEY = "index.engine.exception.ratio.low";
    public static final Setting<Double> EXCEPTION_TOP_LEVEL_RATIO_SETTING =
        Setting.doubleSetting(EXCEPTION_TOP_LEVEL_RATIO_KEY, 0.1d, 0.0d, Setting.Property.IndexScope);
    public static final Setting<Double> EXCEPTION_LOW_LEVEL_RATIO_SETTING =
        Setting.doubleSetting(EXCEPTION_LOW_LEVEL_RATIO_KEY, 0.1d, 0.0d, Setting.Property.IndexScope);

    public static PluginSettings getPluginSettings(Settings settings) {
        return new PluginSettings() {
            @Override
            public List<Setting<?>> getDeclaredSettings() {
                return Arrays.asList(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE, MockEngineSupport.WRAP_READER_RATIO,
                    EXCEPTION_LOW_LEVEL_RATIO_SETTING, EXCEPTION_TOP_LEVEL_RATIO_SETTING);
            }
        };
    }

    @Override
    public void onIndexModule(IndexModule module) {
        module.engineFactory.set(new MockEngineFactory(getReaderWrapperClass()));
    }

    protected Class<? extends FilterDirectoryReader> getReaderWrapperClass() {
        return AssertingDirectoryReader.class;
    }
}
