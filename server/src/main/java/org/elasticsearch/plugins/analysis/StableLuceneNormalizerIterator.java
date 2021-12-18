/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.apache.lucene.analysis.Analyzer;

import java.util.Iterator;
import java.util.List;

public class StableLuceneNormalizerIterator implements PortableAnalyzeIterator {
    private final List<AnalyzeToken> tokens;
    private Iterator<AnalyzeToken> tokenIterator;
    private final Analyzer analyzer;
    private final AnalyzeSettings settings;
    private StableLuceneFilterIterator filterIterator;

    private int lastPosition;
    private int lastOffset;

    public StableLuceneNormalizerIterator(
        Analyzer analyzer,
        List<AnalyzeToken> tokens,
        AnalyzeState prevState,
        AnalyzeSettings settings) {
        this.analyzer = analyzer;
        this.tokens = tokens;
        this.settings = settings;
        tokenIterator = tokens.listIterator();

        lastOffset = prevState.getLastOffset();
        lastPosition = prevState.getLastPosition();
    }

    @Override
    public AnalyzeToken reset() {
        tokenIterator = tokens.listIterator();
        return null;
    }

    @Override
    public AnalyzeToken next() {
        if (filterIterator != null) {
            AnalyzeToken nextFromFilter = filterIterator.next();
            if (nextFromFilter != null) {
                return nextFromFilter;
            } else {
                AnalyzeState finalFilterState = filterIterator.state();
                lastPosition = finalFilterState.getLastPosition();
                lastOffset = finalFilterState.getLastOffset();

                filterIterator.end();
                filterIterator.close();
            }
        }
        if (tokenIterator.hasNext() == false) {
            return null;
        }

        AnalyzeToken nextToken = tokenIterator.next();

        filterIterator = new StableLuceneFilterIterator(
            analyzer.tokenStream(null, nextToken.getTerm()),
            this.state(),
            settings);
        filterIterator.reset();

        return filterIterator.next();
    }

    @Override
    public AnalyzeToken end() {
        if (filterIterator != null) {
            filterIterator.end();
        }
        return null;
    }

    @Override
    public void close() {
        if (filterIterator != null) {
            filterIterator.close();
        }
    }

    @Override
    public AnalyzeState state() {
        return new AnalyzeState(lastPosition, lastOffset);
    }
}
