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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

import java.util.Objects;

/**
 * Named analyzer is an analyzer wrapper around an actual analyzer ({@link #analyzer} that is associated
 * with a name ({@link #name()}.
 */
public class NamedAnalyzer extends DelegatingAnalyzerWrapper {

    private final String name;
    private final AnalyzerScope scope;
    private final Analyzer analyzer;
    private final int positionIncrementGap;

    public NamedAnalyzer(NamedAnalyzer analyzer, int positionIncrementGap) {
        this(analyzer.name(), analyzer.scope(), analyzer.analyzer(), positionIncrementGap);
    }

    public NamedAnalyzer(String name, AnalyzerScope scope, Analyzer analyzer) {
        this(name, scope, analyzer, Integer.MIN_VALUE);
    }

    public NamedAnalyzer(String name, AnalyzerScope scope, Analyzer analyzer, int positionIncrementGap) {
        super(ERROR_STRATEGY);
        this.name = name;
        this.scope = scope;
        this.analyzer = analyzer;
        this.positionIncrementGap = positionIncrementGap;
    }

    /**
     * The name of the analyzer.
     */
    public String name() {
        return this.name;
    }

    /**
     * The scope of the analyzer.
     */
    public AnalyzerScope scope() {
        return this.scope;
    }

    /**
     * The actual analyzer.
     */
    public Analyzer analyzer() {
        return this.analyzer;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return this.analyzer;
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        if (positionIncrementGap != Integer.MIN_VALUE) {
            return positionIncrementGap;
        }
        return super.getPositionIncrementGap(fieldName);
    }

    @Override
    public String toString() {
        return "analyzer name[" + name + "], analyzer [" + analyzer + "]";
    }

    /** It is an error if this is ever used, it means we screwed up! */
    static final ReuseStrategy ERROR_STRATEGY = new Analyzer.ReuseStrategy() {
        @Override
        public TokenStreamComponents getReusableComponents(Analyzer a, String f) {
            throw new IllegalStateException("NamedAnalyzer cannot be wrapped with a wrapper, only a delegator");
        }

        @Override
        public void setReusableComponents(Analyzer a, String f, TokenStreamComponents c) {
            throw new IllegalStateException("NamedAnalyzer cannot be wrapped with a wrapper, only a delegator");
        }
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NamedAnalyzer)) return false;
        NamedAnalyzer that = (NamedAnalyzer) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public void close() {
        super.close();
        if (scope == AnalyzerScope.INDEX) {
            analyzer.close();
        }
    }
}
