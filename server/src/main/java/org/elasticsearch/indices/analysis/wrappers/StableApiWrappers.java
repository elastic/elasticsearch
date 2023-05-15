/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis.wrappers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugin.Inject;
import org.elasticsearch.plugin.analysis.AnalysisMode;
import org.elasticsearch.plugin.analysis.AnalyzerFactory;
import org.elasticsearch.plugin.analysis.CharFilterFactory;
import org.elasticsearch.plugin.analysis.TokenFilterFactory;
import org.elasticsearch.plugin.analysis.TokenizerFactory;
import org.elasticsearch.plugin.settings.AnalysisSettings;
import org.elasticsearch.plugins.scanners.PluginInfo;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A utility class containing methods that wraps the Stable plugin API with the old plugin api.
 * Note that most old and stable api classes have the same names but differ in package name.
 * Hence this class is avoiding imports and is using qualifying names
 */
public class StableApiWrappers {
    public static
        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.CharFilterFactory>>
        oldApiForStableCharFilterFactory(StablePluginsRegistry stablePluginRegistry) {
        return mapStablePluginApiToOld(stablePluginRegistry, CharFilterFactory.class, StableApiWrappers::wrapCharFilterFactory);
    }

    public static
        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.TokenFilterFactory>>
        oldApiForTokenFilterFactory(StablePluginsRegistry stablePluginRegistry) {
        return mapStablePluginApiToOld(stablePluginRegistry, TokenFilterFactory.class, StableApiWrappers::wrapTokenFilterFactory);
    }

    public static Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.TokenizerFactory>> oldApiForTokenizerFactory(
        StablePluginsRegistry stablePluginRegistry
    ) {
        return mapStablePluginApiToOld(stablePluginRegistry, TokenizerFactory.class, StableApiWrappers::wrapTokenizerFactory);
    }

    public static
        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.AnalyzerProvider<?>>>
        oldApiForAnalyzerFactory(StablePluginsRegistry stablePluginRegistry) {
        return mapStablePluginApiToOld(stablePluginRegistry, AnalyzerFactory.class, StableApiWrappers::wrapAnalyzerFactory);
    }

    private static <T, F> Map<String, AnalysisModule.AnalysisProvider<T>> mapStablePluginApiToOld(
        StablePluginsRegistry stablePluginRegistry,
        Class<F> charFilterFactoryClass,
        Function<F, T> wrapper
    ) {
        Collection<PluginInfo> pluginInfosForExtensible = stablePluginRegistry.getPluginInfosForExtensible(
            charFilterFactoryClass.getCanonicalName()
        );

        Map<String, AnalysisModule.AnalysisProvider<T>> oldApiComponents = pluginInfosForExtensible.stream()
            .collect(Collectors.toMap(PluginInfo::name, p -> analysisProviderWrapper(p, wrapper)));
        return oldApiComponents;
    }

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
                    throw new IllegalStateException("Plugin classloader cannot find class " + pluginInfo.className(), e);
                }
            }
        };
    }

    private static org.elasticsearch.index.analysis.CharFilterFactory wrapCharFilterFactory(CharFilterFactory charFilterFactory) {
        return new org.elasticsearch.index.analysis.CharFilterFactory() {
            @Override
            public String name() {
                return charFilterFactory.name();
            }

            @Override
            public Reader create(Reader reader) {
                return charFilterFactory.create(reader);
            }

            @Override
            public Reader normalize(Reader reader) {
                return charFilterFactory.normalize(reader);
            }
        };
    }

    private static org.elasticsearch.index.analysis.TokenFilterFactory wrapTokenFilterFactory(TokenFilterFactory f) {
        return new org.elasticsearch.index.analysis.TokenFilterFactory() {
            @Override
            public String name() {
                return f.name();
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return f.create(tokenStream);
            }

            @Override
            public TokenStream normalize(TokenStream tokenStream) {
                return f.normalize(tokenStream);
            }

            @Override
            public org.elasticsearch.index.analysis.AnalysisMode getAnalysisMode() {
                return mapAnalysisMode(f.getAnalysisMode());
            }

            private org.elasticsearch.index.analysis.AnalysisMode mapAnalysisMode(AnalysisMode analysisMode) {
                return org.elasticsearch.index.analysis.AnalysisMode.valueOf(analysisMode.name());
            }
        };
    }

    private static org.elasticsearch.index.analysis.TokenizerFactory wrapTokenizerFactory(TokenizerFactory f) {
        return new org.elasticsearch.index.analysis.TokenizerFactory() {

            @Override
            public String name() {
                return f.name();
            }

            @Override
            public Tokenizer create() {
                return f.create();
            }
        };
    }

    private static org.elasticsearch.index.analysis.AnalyzerProvider<?> wrapAnalyzerFactory(AnalyzerFactory f) {
        return new org.elasticsearch.index.analysis.AnalyzerProvider<>() {
            @Override
            public String name() {
                return f.name();
            }

            @Override
            public org.elasticsearch.index.analysis.AnalyzerScope scope() {
                return org.elasticsearch.index.analysis.AnalyzerScope.GLOBAL;// TODO is this right?
            }

            @Override
            public Analyzer get() {
                return f.create();
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

            Constructor<?>[] constructors = clazz.getConstructors();
            if (constructors.length > 1) {
                throw new IllegalStateException("Plugin can only have one public constructor.");
            }
            Constructor<?> constructor = constructors[0];
            if (constructor.getParameterCount() == 0) {
                return (T) constructor.newInstance();
            } else {
                Inject inject = constructor.getAnnotation(Inject.class);
                if (inject != null) {
                    Class<?>[] parameterTypes = constructor.getParameterTypes();
                    Object[] parameters = new Object[parameterTypes.length];
                    for (int i = 0; i < parameterTypes.length; i++) {
                        Object settings = createSettings(parameterTypes[i], indexSettings, nodeSettings, analysisSettings, environment);
                        parameters[i] = settings;
                    }
                    return (T) constructor.newInstance(parameters);
                } else {
                    throw new IllegalStateException("Missing @Inject annotation for constructor with settings.");
                }
            }

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Cannot create instance of " + clazz, e);
        }

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static <T> T createSettings(
        Class<T> settingsClass,
        IndexSettings indexSettings,
        Settings nodeSettings,
        Settings analysisSettings,
        Environment environment
    ) {
        if (settingsClass.getAnnotationsByType(AnalysisSettings.class).length > 0) {
            return SettingsInvocationHandler.create(analysisSettings, settingsClass, environment);
        }

        throw new IllegalArgumentException("Parameter is not instance of a class annotated with settings annotation.");
    }
}
