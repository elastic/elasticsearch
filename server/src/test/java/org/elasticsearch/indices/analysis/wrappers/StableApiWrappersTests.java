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
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.analysis.AnalysisMode;
import org.elasticsearch.plugin.analysis.AnalyzerFactory;
import org.elasticsearch.plugin.analysis.CharFilterFactory;
import org.elasticsearch.plugin.analysis.TokenFilterFactory;
import org.elasticsearch.plugin.analysis.TokenizerFactory;
import org.elasticsearch.plugins.scanners.PluginInfo;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class StableApiWrappersTests extends ESTestCase {

    public void testUnknownClass() throws IOException {
        StablePluginsRegistry registry = Mockito.mock(StablePluginsRegistry.class);
        Mockito.when(registry.getPluginInfosForExtensible(eq(AnalyzerFactory.class.getCanonicalName())))
            .thenReturn(List.of(new PluginInfo("namedComponentName1", "someRandomName", getClass().getClassLoader())));

        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.AnalyzerProvider<?>>> analysisProviderMap =
            StableApiWrappers.oldApiForAnalyzerFactory(registry);

        AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.AnalyzerProvider<?>> oldTokenFilter = analysisProviderMap.get(
            "namedComponentName1"
        );

        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> oldTokenFilter.get(null, mock(Environment.class), null, null)
        );
        assertThat(illegalStateException.getCause(), instanceOf(ClassNotFoundException.class));
    }

    public void testStablePluginHasNoArgConstructor() throws IOException {
        StablePluginsRegistry registry = Mockito.mock(StablePluginsRegistry.class);
        Mockito.when(registry.getPluginInfosForExtensible(eq(AnalyzerFactory.class.getCanonicalName())))
            .thenReturn(
                List.of(new PluginInfo("namedComponentName1", DefaultConstrAnalyzerFactory.class.getName(), getClass().getClassLoader()))
            );

        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.AnalyzerProvider<?>>> analysisProviderMap =
            StableApiWrappers.oldApiForAnalyzerFactory(registry);

        AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.AnalyzerProvider<?>> oldTokenFilter = analysisProviderMap.get(
            "namedComponentName1"
        );

        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> oldTokenFilter.get(null, mock(Environment.class), null, null)
        );
        assertThat(illegalStateException.getMessage(), equalTo("Missing @Inject annotation for constructor with settings."));
    }

    public void testAnalyzerFactoryDelegation() throws IOException {
        StablePluginsRegistry registry = Mockito.mock(StablePluginsRegistry.class);
        Mockito.when(registry.getPluginInfosForExtensible(eq(AnalyzerFactory.class.getCanonicalName())))
            .thenReturn(List.of(new PluginInfo("namedComponentName1", TestAnalyzerFactory.class.getName(), getClass().getClassLoader())));

        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.AnalyzerProvider<?>>> analysisProviderMap =
            StableApiWrappers.oldApiForAnalyzerFactory(registry);

        AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.AnalyzerProvider<?>> oldTokenFilter = analysisProviderMap.get(
            "namedComponentName1"
        );

        org.elasticsearch.index.analysis.AnalyzerProvider<?> analyzerProvider = oldTokenFilter.get(
            null,
            mock(Environment.class),
            null,
            null
        );

        // test delegation
        Analyzer analyzer = analyzerProvider.get();
        assertTrue(Mockito.mockingDetails(analyzer).isMock());

        assertThat(analyzerProvider.name(), equalTo("TestAnalyzerFactory"));
        assertThat(analyzerProvider.scope(), equalTo(AnalyzerScope.GLOBAL));
    }

    public void testTokenizerFactoryDelegation() throws IOException {
        StablePluginsRegistry registry = Mockito.mock(StablePluginsRegistry.class);
        Mockito.when(registry.getPluginInfosForExtensible(eq(TokenizerFactory.class.getCanonicalName())))
            .thenReturn(List.of(new PluginInfo("namedComponentName1", TestTokenizerFactory.class.getName(), getClass().getClassLoader())));

        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.TokenizerFactory>> analysisProviderMap =
            StableApiWrappers.oldApiForTokenizerFactory(registry);

        AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.TokenizerFactory> oldTokenFilter = analysisProviderMap.get(
            "namedComponentName1"
        );

        org.elasticsearch.index.analysis.TokenizerFactory tokenizerFactory = oldTokenFilter.get(null, mock(Environment.class), null, null);

        // test delegation
        Tokenizer tokenizer = tokenizerFactory.create();

        assertTrue(Mockito.mockingDetails(tokenizer).isMock());

        assertThat(tokenizerFactory.name(), equalTo("TestTokenizerFactory"));
    }

    public void testTokenFilterFactoryDelegation() throws IOException {
        StablePluginsRegistry registry = Mockito.mock(StablePluginsRegistry.class);
        Mockito.when(registry.getPluginInfosForExtensible(eq(TokenFilterFactory.class.getCanonicalName())))
            .thenReturn(
                List.of(new PluginInfo("namedComponentName1", TestTokenFilterFactory.class.getName(), getClass().getClassLoader()))
            );

        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.TokenFilterFactory>> analysisProviderMap =
            StableApiWrappers.oldApiForTokenFilterFactory(registry);

        AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.TokenFilterFactory> oldTokenFilter = analysisProviderMap.get(
            "namedComponentName1"
        );

        org.elasticsearch.index.analysis.TokenFilterFactory tokenFilterFactory = oldTokenFilter.get(
            null,
            mock(Environment.class),
            null,
            null
        );

        // test delegation
        TokenStream createTokenStreamMock = mock(TokenStream.class);
        TokenStream tokenStream = tokenFilterFactory.create(createTokenStreamMock);

        assertSame(tokenStream, createTokenStreamMock);
        verify(createTokenStreamMock).incrementToken();

        TokenStream normalizeTokenStreamMock = mock(TokenStream.class);
        tokenStream = tokenFilterFactory.normalize(normalizeTokenStreamMock);

        assertSame(tokenStream, normalizeTokenStreamMock);
        verify(normalizeTokenStreamMock).incrementToken();

        assertThat(tokenFilterFactory.getAnalysisMode(), equalTo(org.elasticsearch.index.analysis.AnalysisMode.INDEX_TIME));

        assertThat(tokenFilterFactory.name(), equalTo("TestTokenFilterFactory"));
    }

    public void testCharFilterFactoryDelegation() throws IOException {
        StablePluginsRegistry registry = Mockito.mock(StablePluginsRegistry.class);
        Mockito.when(registry.getPluginInfosForExtensible(eq(CharFilterFactory.class.getCanonicalName())))
            .thenReturn(List.of(new PluginInfo("namedComponentName1", TestCharFilterFactory.class.getName(), getClass().getClassLoader())));

        Map<String, AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.CharFilterFactory>> analysisProviderMap =
            StableApiWrappers.oldApiForStableCharFilterFactory(registry);

        AnalysisModule.AnalysisProvider<org.elasticsearch.index.analysis.CharFilterFactory> oldCharFilter = analysisProviderMap.get(
            "namedComponentName1"
        );

        org.elasticsearch.index.analysis.CharFilterFactory charFilterFactory = oldCharFilter.get(null, mock(Environment.class), null, null);

        // test delegation
        Reader createReaderMock = mock(Reader.class);
        Reader reader = charFilterFactory.create(createReaderMock);

        assertSame(reader, createReaderMock);
        verify(createReaderMock).read();

        Reader normalizeReaderMock = mock(Reader.class);
        reader = charFilterFactory.normalize(normalizeReaderMock);

        assertSame(reader, normalizeReaderMock);
        verify(normalizeReaderMock).read();

        assertThat(charFilterFactory.name(), equalTo("TestCharFilterFactory"));
    }

    @NamedComponent("DefaultConstrAnalyzerFactory")
    public static class DefaultConstrAnalyzerFactory implements AnalyzerFactory {

        public DefaultConstrAnalyzerFactory(int x) {}

        @Override
        public Analyzer create() {
            return null;
        }

    }

    @NamedComponent("TestAnalyzerFactory")
    public static class TestAnalyzerFactory implements AnalyzerFactory {

        @Override
        public Analyzer create() {
            return Mockito.mock(Analyzer.class);
        }

    }

    @NamedComponent("TestTokenizerFactory")
    public static class TestTokenizerFactory implements TokenizerFactory {

        @Override
        public Tokenizer create() {
            return Mockito.mock(Tokenizer.class);
        }
    }

    @NamedComponent("TestTokenFilterFactory")
    public static class TestTokenFilterFactory implements TokenFilterFactory {

        @Override
        public TokenStream create(TokenStream tokenStream) {
            try {
                tokenStream.incrementToken();
            } catch (IOException e) {}
            return tokenStream;
        }

        @Override
        public TokenStream normalize(TokenStream tokenStream) {
            try {
                tokenStream.incrementToken();
            } catch (IOException e) {}
            return tokenStream;
        }

        @Override
        public AnalysisMode getAnalysisMode() {
            return AnalysisMode.INDEX_TIME;
        }
    }

    @NamedComponent("TestCharFilterFactory")
    public static class TestCharFilterFactory implements CharFilterFactory {

        @Override
        public Reader create(Reader reader) {
            try {
                reader.read();
            } catch (IOException e) {}
            return reader;
        }

        @Override
        public Reader normalize(Reader reader) {
            try {
                reader.read();
            } catch (IOException e) {}
            return reader;
        }

    }
}
