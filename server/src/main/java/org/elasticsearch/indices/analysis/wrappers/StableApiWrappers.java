/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis.wrappers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugin.analysis.api.CharFilterFactory;
import org.elasticsearch.plugins.scanners.PluginInfo;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StableApiWrappers {
    public static
        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.CharFilterFactory>>
        oldApiForStableCharFilterFactory(StablePluginsRegistry stablePluginRegistry) {
        Collection<PluginInfo> pluginInfosForExtensible = stablePluginRegistry.getPluginInfosForExtensible(
            CharFilterFactory.class.getCanonicalName()
        );

        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.CharFilterFactory>> oldCharFilters =
            pluginInfosForExtensible.stream()
                .collect(Collectors.toMap(PluginInfo::name, p -> analysisProviderWrapper(p, StableApiWrappers::wrapCharFilterFactory)));

        return oldCharFilters;
    }
    // StableApiWrappers.<CharFilterFactory, org.elasticsearch.index.analysis.CharFilterFactory>analysisProviderWrapper

    /*
    ableApiWrappers.java:49: warning: [unchecked] unchecked cast
                            pluginInfo.loader().loadClass(pluginInfo.className());
                                                         ^
      required: Class<? extends F>
      found:    Class<CAP#1>
      where F,T are type-variables:
        F extends Object declared in method <F,T>analysisProviderWrapper(PluginInfo,Function<F,T>)
        T extends Object declared in method <F,T>analysisProviderWrapper(PluginInfo,Function<F,T>)
      where CAP#1 is a fresh type-variable:
        CAP#1 extends Object from capture of ?

     */
    @SuppressWarnings("unchecked")
    private static <F, T> AnalysisModule.AnalysisProvider<T> analysisProviderWrapper(PluginInfo pluginInfo, Function<F, T> wrapper) {
        return new AnalysisModule.AnalysisProvider<T>() {

            @Override
            public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                try {
                    Class<? extends F> clazz = (Class<? extends F>) pluginInfo.loader().loadClass(pluginInfo.className());
                    F instance = createInstance(clazz, indexSettings, environment.settings(), settings, environment);
                    return wrapper.apply(instance);
                } catch (ClassNotFoundException e) {
                    // e.printStackTrace();
                }
                return null;
            }
        };
    }

    static org.elasticsearch.index.analysis.CharFilterFactory wrapCharFilterFactory(CharFilterFactory charFilterFactory) {
        return new org.elasticsearch.index.analysis.CharFilterFactory() {
            @Override
            public String name() {
                return charFilterFactory.name();
            }

            @Override
            public Reader create(Reader reader) {
                return charFilterFactory.create(reader);
            }
        };
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static <T> T createInstance(
        Class<T> clazz,
        IndexSettings indexSettings,
        Settings nodeSettings,
        Settings analysisSettings,
        Environment environment
    ) {

        try {
            Constructor<T> constructor = clazz.getConstructor();
            return constructor.newInstance();

        } catch (Exception e) {
            // e.printStackTrace();
        }
        throw new RuntimeException("cannot create instance of " + clazz + ", no injectable ctr found");
    }
}
