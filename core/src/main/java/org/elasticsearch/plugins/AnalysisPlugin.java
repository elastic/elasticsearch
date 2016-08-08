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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;

import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * An additional extension point for {@link Plugin}s that extends Elasticsearch's analysis functionality. To add an additional
 * {@link TokenFilter} just implement the interface and implement the {@link #getTokenFilters()} method:
 *
 * <pre>{@code
 * public class AnalysisPhoneticPlugin extends Plugin implements AnalysisPlugin {
 *     &#64;Override
 *     public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
 *         return singletonMap("phonetic", PhoneticTokenFilterFactory::new);
 *     }
 * }
 * }</pre>
 *
 * Elasticsearch doesn't have any automatic mechanism to share these components between indexes. If any component is heavy enough to warrant
 * such sharing then it is the Pugin's responsibility to do it in their {@link AnalysisProvider} implementation. We recommend against doing
 * this unless absolutely necessary because it can be difficult to get the caching right given things like behavior changes across versions.
 */
public interface AnalysisPlugin {
    /**
     * Override to add additional {@link CharFilter}s.
     */
    default Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link TokenFilter}s.
     */
    default Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link Tokenizer}s.
     */
    default Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link Analyzer}s.
     */
    default Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return emptyMap();
    }

    /**
     * Override to add additional hunspell {@link org.apache.lucene.analysis.hunspell.Dictionary}s.
     */
    default Map<String, org.apache.lucene.analysis.hunspell.Dictionary> getHunspellDictionaries() {
        return emptyMap();
    }
}
