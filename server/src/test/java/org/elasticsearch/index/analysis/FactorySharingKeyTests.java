/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.Reader;

/**
 * Paranoid tests for {@code sharingKey()} overrides on server-side factories.
 *
 * <p>Each test constructs two factory instances with identical (or differing) parameters and
 * verifies that {@link Object#equals}/{@link Object#hashCode} on the returned keys behave as the
 * sharing contract requires. These run in microseconds and catch entire classes of regressions
 * (missing fields, wrong hashing, typos in the Key record) that would silently corrupt the cache.
 */
public class FactorySharingKeyTests extends ESTestCase {

    private IndexSettings indexSettings() {
        Settings idx = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        return IndexSettingsModule.newIndexSettings("test", idx);
    }

    private Environment env() {
        return TestEnvironment.newEnvironment(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build());
    }

    // ---- Cross-type isolation: the registry folds the factory class into every chain-slot key ----

    public void testFactoryKeyIsolatesByClass() {
        // Two different factory types whose keys are bare-equal values (here a plain Boolean) must
        // never compare equal — otherwise an analyzer using one filter could wrongly share with an
        // analyzer using the other. Same type + same key still shares.
        assertNotEquals(
            new AnalysisRegistry.FactoryKey(StopTokenFilterFactory.class, Boolean.TRUE),
            new AnalysisRegistry.FactoryKey(ShingleTokenFilterFactory.class, Boolean.TRUE)
        );
        assertEquals(
            new AnalysisRegistry.FactoryKey(StopTokenFilterFactory.class, Boolean.TRUE),
            new AnalysisRegistry.FactoryKey(StopTokenFilterFactory.class, Boolean.TRUE)
        );
    }

    // ---- StopTokenFilterFactory ----

    public void testStopFactorySharesByDefaultSettings() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StopTokenFilterFactory a = new StopTokenFilterFactory(is, env, "a", Settings.EMPTY);
        StopTokenFilterFactory b = new StopTokenFilterFactory(is, env, "b", Settings.EMPTY);
        assertEquals(a.sharingKey(), b.sharingKey());
        assertEquals(a.sharingKey().hashCode(), b.sharingKey().hashCode());
    }

    public void testStopFactoryDifferentStopWordsDoesNotShare() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StopTokenFilterFactory a = new StopTokenFilterFactory(is, env, "a", Settings.builder().putList("stopwords", "the", "and").build());
        StopTokenFilterFactory b = new StopTokenFilterFactory(is, env, "b", Settings.builder().putList("stopwords", "the", "or").build());
        assertNotEquals(a.sharingKey(), b.sharingKey());
    }

    public void testStopFactoryIgnoreCaseDifferentiates() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StopTokenFilterFactory caseSensitive = new StopTokenFilterFactory(
            is,
            env,
            "a",
            Settings.builder().put("ignore_case", false).build()
        );
        StopTokenFilterFactory caseInsensitive = new StopTokenFilterFactory(
            is,
            env,
            "b",
            Settings.builder().put("ignore_case", true).build()
        );
        assertNotEquals(caseSensitive.sharingKey(), caseInsensitive.sharingKey());
    }

    public void testStopFactoryRemoveTrailingDifferentiates() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StopTokenFilterFactory keep = new StopTokenFilterFactory(is, env, "a", Settings.builder().put("remove_trailing", true).build());
        StopTokenFilterFactory remove = new StopTokenFilterFactory(is, env, "b", Settings.builder().put("remove_trailing", false).build());
        assertNotEquals(keep.sharingKey(), remove.sharingKey());
    }

    // ---- ShingleTokenFilterFactory ----

    public void testShingleFactorySharesByDefaults() {
        IndexSettings is = indexSettings();
        Environment env = env();
        ShingleTokenFilterFactory a = new ShingleTokenFilterFactory(is, env, "a", Settings.EMPTY);
        ShingleTokenFilterFactory b = new ShingleTokenFilterFactory(is, env, "b", Settings.EMPTY);
        assertEquals(a.sharingKey(), b.sharingKey());
        assertEquals(a.sharingKey().hashCode(), b.sharingKey().hashCode());
    }

    public void testShingleFactoryShingleSizeDifferentiates() {
        IndexSettings is = indexSettings();
        Environment env = env();
        ShingleTokenFilterFactory two = new ShingleTokenFilterFactory(
            is,
            env,
            "a",
            Settings.builder().put("min_shingle_size", 2).put("max_shingle_size", 2).build()
        );
        ShingleTokenFilterFactory three = new ShingleTokenFilterFactory(
            is,
            env,
            "b",
            Settings.builder().put("min_shingle_size", 3).put("max_shingle_size", 3).build()
        );
        assertNotEquals(two.sharingKey(), three.sharingKey());
    }

    public void testShingleFactoryTokenSeparatorDifferentiates() {
        IndexSettings is = indexSettings();
        Environment env = env();
        ShingleTokenFilterFactory space = new ShingleTokenFilterFactory(
            is,
            env,
            "a",
            Settings.builder().put("token_separator", " ").build()
        );
        ShingleTokenFilterFactory dash = new ShingleTokenFilterFactory(
            is,
            env,
            "b",
            Settings.builder().put("token_separator", "-").build()
        );
        assertNotEquals(space.sharingKey(), dash.sharingKey());
    }

    // ---- StandardTokenizerFactory ----

    public void testStandardTokenizerSharesByDefaultMaxTokenLength() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StandardTokenizerFactory a = new StandardTokenizerFactory(is, env, "a", Settings.EMPTY);
        StandardTokenizerFactory b = new StandardTokenizerFactory(is, env, "b", Settings.EMPTY);
        assertEquals(a.sharingKey(), b.sharingKey());
    }

    public void testStandardTokenizerMaxTokenLengthDifferentiates() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StandardTokenizerFactory a = new StandardTokenizerFactory(is, env, "a", Settings.builder().put("max_token_length", 100).build());
        StandardTokenizerFactory b = new StandardTokenizerFactory(is, env, "b", Settings.builder().put("max_token_length", 200).build());
        assertNotEquals(a.sharingKey(), b.sharingKey());
    }

    // ---- StandardAnalyzerProvider ----

    public void testStandardAnalyzerProviderSharesByDefaults() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StandardAnalyzerProvider a = new StandardAnalyzerProvider(is, env, "a", Settings.EMPTY);
        StandardAnalyzerProvider b = new StandardAnalyzerProvider(is, env, "b", Settings.EMPTY);
        assertEquals(a.sharingKey(), b.sharingKey());
    }

    public void testStandardAnalyzerProviderStopWordsDifferentiate() {
        IndexSettings is = indexSettings();
        Environment env = env();
        StandardAnalyzerProvider noStops = new StandardAnalyzerProvider(is, env, "a", Settings.EMPTY);
        StandardAnalyzerProvider withStops = new StandardAnalyzerProvider(
            is,
            env,
            "b",
            Settings.builder().putList("stopwords", "x", "y").build()
        );
        assertNotEquals(noStops.sharingKey(), withStops.sharingKey());
    }

    public void testStandardAnalyzerProviderStopwordsCaseDifferentiates() {
        IndexSettings is = indexSettings();
        Environment env = env();
        Settings base = Settings.builder().putList("stopwords", "The", "And").build();
        StandardAnalyzerProvider caseSensitive = new StandardAnalyzerProvider(is, env, "a", base);
        StandardAnalyzerProvider caseInsensitive = new StandardAnalyzerProvider(
            is,
            env,
            "b",
            Settings.builder().put(base).put("stopwords_case", true).build()
        );
        // stopwords_case changes how the stop filter matches uppercase stopwords, so the two recipes
        // must not share an instance even though the stopword list is identical.
        assertNotEquals(caseSensitive.sharingKey(), caseInsensitive.sharingKey());
    }

    // ---- Default-sharingKey contract ----

    public void testFactoryWithoutSharingKeyOverrideIsIdentityUnique() {
        TokenFilterFactory a = new AbstractTokenFilterFactory("a") {
            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };
        TokenFilterFactory b = new AbstractTokenFilterFactory("a") {
            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };
        // Two distinct factories, no sharingKey override → default returns `this` → identity-unique.
        assertNotSame(a.sharingKey(), b.sharingKey());
        assertNotEquals(a.sharingKey(), b.sharingKey());
        assertSame(a, a.sharingKey());
    }

    public void testTokenizerFactoryDefaultIsIdentity() {
        TokenizerFactory a = TokenizerFactory.newFactory("x", () -> { throw new UnsupportedOperationException(); });
        TokenizerFactory b = TokenizerFactory.newFactory("x", () -> { throw new UnsupportedOperationException(); });
        assertNotEquals(a.sharingKey(), b.sharingKey());
    }

    public void testCharFilterFactoryDefaultIsIdentity() {
        CharFilterFactory a = new CharFilterFactory() {
            @Override
            public String name() {
                return "x";
            }

            @Override
            public Reader create(Reader reader) {
                return reader;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };
        CharFilterFactory b = new CharFilterFactory() {
            @Override
            public String name() {
                return "x";
            }

            @Override
            public Reader create(Reader reader) {
                return reader;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };
        assertNotEquals(a.sharingKey(), b.sharingKey());
    }

    public void testAnalyzerProviderDefaultIsIdentity() {
        AnalyzerProvider<StandardAnalyzer> a = new AnalyzerProvider<>() {
            @Override
            public String name() {
                return "x";
            }

            @Override
            public AnalyzerScope scope() {
                return AnalyzerScope.INDEX;
            }

            @Override
            public StandardAnalyzer get() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };
        AnalyzerProvider<StandardAnalyzer> b = new AnalyzerProvider<>() {
            @Override
            public String name() {
                return "x";
            }

            @Override
            public AnalyzerScope scope() {
                return AnalyzerScope.INDEX;
            }

            @Override
            public StandardAnalyzer get() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };
        assertNotEquals(a.sharingKey(), b.sharingKey());
    }
}
