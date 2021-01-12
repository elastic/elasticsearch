/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter;

/**
 * This analyzer limits the highlighting once it sees a token with a start offset &lt;= the configured limit,
 * which won't pass and will end the stream.
 * @see LimitTokenOffsetFilter
 */
public final class LimitTokenOffsetAnalyzer extends AnalyzerWrapper {

    private final Analyzer delegate;
    private final int maxOffset;

    /**
     * Build an analyzer that limits the highlighting once it sees a token with a start offset &lt;= the configured limit,
     * which won't pass and will end the stream. See {@link LimitTokenOffsetFilter} for more details.
     *
     * @param delegate the analyzer to wrap
     * @param maxOffset max number of tokens to produce
     */
    public LimitTokenOffsetAnalyzer(Analyzer delegate, int maxOffset) {
        super(delegate.getReuseStrategy());
        this.delegate = delegate;
        this.maxOffset = maxOffset;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return delegate;
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return new TokenStreamComponents(
                components.getSource(),
                new LimitTokenOffsetFilter(components.getTokenStream(), maxOffset, false)
        );
    }

    @Override
    public String toString() {
        return "LimitTokenOffsetAnalyzer("
                + delegate.toString()
                + ", maxOffset="
                + maxOffset
                + ")";
    }
}
