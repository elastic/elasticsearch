/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.elasticsearch.index.mapper.MapperException;

import java.util.ArrayList;
import java.util.List;
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
    private final AnalysisMode analysisMode;

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
        if (analyzer instanceof org.elasticsearch.index.analysis.AnalyzerComponentsProvider) {
            this.analysisMode = ((org.elasticsearch.index.analysis.AnalyzerComponentsProvider) analyzer).getComponents().analysisMode();
        } else {
            this.analysisMode = AnalysisMode.ALL;
        }
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
     * Returns whether this analyzer can be updated
     */
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
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

    /**
     * Checks the wrapped analyzer for the provided restricted {@link AnalysisMode} and throws
     * an error if the analyzer is not allowed to run in that mode. The error contains more detailed information about
     * the offending filters that caused the analyzer to not be allowed in this mode.
     */
    public void checkAllowedInMode(AnalysisMode mode) {
        Objects.requireNonNull(mode);
        if (this.getAnalysisMode() == AnalysisMode.ALL) {
            return; // everything allowed if this analyzer is in ALL mode
        }
        if (this.getAnalysisMode() != mode) {
            if (analyzer instanceof AnalyzerComponentsProvider) {
                TokenFilterFactory[] tokenFilters = ((AnalyzerComponentsProvider) analyzer).getComponents().getTokenFilters();
                List<String> offendingFilters = new ArrayList<>();
                for (TokenFilterFactory tokenFilter : tokenFilters) {
                    AnalysisMode filterMode = tokenFilter.getAnalysisMode();
                    if (filterMode != AnalysisMode.ALL && filterMode != mode) {
                        offendingFilters.add(tokenFilter.name());
                    }
                }
                throw new MapperException(
                    "analyzer ["
                        + name
                        + "] contains filters "
                        + offendingFilters
                        + " that are not allowed to run in "
                        + mode.getReadableName()
                        + " mode."
                );
            } else {
                throw new MapperException(
                    "analyzer [" + name + "] contains components that are not allowed to run in " + mode.getReadableName() + " mode."
                );
            }
        }
    }

    public boolean containsBrokenAnalysis() {
        if (analyzer instanceof AnalyzerComponentsProvider) {
            final TokenFilterFactory[] tokenFilters = ((AnalyzerComponentsProvider) analyzer).getComponents().getTokenFilters();
            for (TokenFilterFactory tokenFilterFactory : tokenFilters) {
                if (tokenFilterFactory.breaksFastVectorHighlighter()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "analyzer name[" + name + "], analyzer [" + analyzer + "], analysisMode [" + analysisMode + "]";
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
        if ((o instanceof NamedAnalyzer) == false) return false;
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
