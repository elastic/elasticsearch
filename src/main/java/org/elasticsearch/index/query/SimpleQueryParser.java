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
package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import java.util.Locale;
import java.util.Map;

/**
 * Wrapper class for Lucene's SimpleQueryParser that allows us to redefine
 * different types of queries.
 */
public class SimpleQueryParser extends org.apache.lucene.queryparser.simple.SimpleQueryParser {

    private final Settings settings;

    /** Creates a new parser with custom flags used to enable/disable certain features. */
    public SimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags, Settings settings) {
        super(analyzer, weights, flags);
        this.settings = settings;
    }

    /**
     * Rethrow the runtime exception, unless the lenient flag has been set, returns null
     */
    private Query rethrowUnlessLenient(RuntimeException e) {
        if (settings.lenient()) {
            return null;
        }
        throw e;
    }

    @Override
    public Query newDefaultQuery(String text) {
        try {
            return super.newDefaultQuery(text);
        } catch (RuntimeException e) {
            return rethrowUnlessLenient(e);
        }
    }

    /**
     * Dispatches to Lucene's SimpleQueryParser's newFuzzyQuery, optionally
     * lowercasing the term first
     */
    @Override
    public Query newFuzzyQuery(String text, int fuzziness) {
        if (settings.lowercaseExpandedTerms()) {
            text = text.toLowerCase(settings.locale());
        }
        try {
            return super.newFuzzyQuery(text, fuzziness);
        } catch (RuntimeException e) {
            return rethrowUnlessLenient(e);
        }
    }

    @Override
    public Query newPhraseQuery(String text, int slop) {
        try {
            return super.newPhraseQuery(text, slop);
        } catch (RuntimeException e) {
            return rethrowUnlessLenient(e);
        }
    }

    /**
     * Dispatches to Lucene's SimpleQueryParser's newPrefixQuery, optionally
     * lowercasing the term first
     */
    @Override
    public Query newPrefixQuery(String text) {
        if (settings.lowercaseExpandedTerms()) {
            text = text.toLowerCase(settings.locale());
        }
        try {
            return super.newPrefixQuery(text);
        } catch (RuntimeException e) {
            return rethrowUnlessLenient(e);
        }
    }

    /**
     * Class encapsulating the settings for the SimpleQueryString query, with
     * their default values
     */
    public static class Settings {
        private Locale locale = Locale.ROOT;
        private boolean lowercaseExpandedTerms = true;
        private boolean lenient = false;

        public Settings() {

        }

        public void locale(Locale locale) {
            this.locale = locale;
        }

        public Locale locale() {
            return this.locale;
        }

        public void lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
            this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        }

        public boolean lowercaseExpandedTerms() {
            return this.lowercaseExpandedTerms;
        }

        public void lenient(boolean lenient) {
            this.lenient = lenient;
        }

        public boolean lenient() {
            return this.lenient;
        }
    }
}
