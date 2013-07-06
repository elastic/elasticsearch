/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.analysis;

import java.io.Reader;

/**
 * Similar to Lucene {@link AnalyzerWrapper} but actually allows to set the reuse strategy....
 * //TODO add to lucene the ability to set it...
 */
public abstract class CustomAnalyzerWrapper extends Analyzer {

    /**
     * Creates a new CustomAnalyzerWrapper.  Since the {@link Analyzer.ReuseStrategy} of
     * the wrapped Analyzers are unknown, {@link Analyzer.PerFieldReuseStrategy} is assumed
     */
    protected CustomAnalyzerWrapper(ReuseStrategy reuseStrategy) {
        super(reuseStrategy);
    }

    /**
     * Retrieves the wrapped Analyzer appropriate for analyzing the field with
     * the given name
     *
     * @param fieldName Name of the field which is to be analyzed
     * @return Analyzer for the field with the given name.  Assumed to be non-null
     */
    protected abstract Analyzer getWrappedAnalyzer(String fieldName);

    /**
     * Wraps / alters the given TokenStreamComponents, taken from the wrapped
     * Analyzer, to form new components.  It is through this method that new
     * TokenFilters can be added by AnalyzerWrappers.
     *
     * @param fieldName  Name of the field which is to be analyzed
     * @param components TokenStreamComponents taken from the wrapped Analyzer
     * @return Wrapped / altered TokenStreamComponents.
     */
    protected abstract TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components);

    @Override
    protected final TokenStreamComponents createComponents(String fieldName, Reader aReader) {
        return wrapComponents(fieldName, getWrappedAnalyzer(fieldName).createComponents(fieldName, aReader));
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return getWrappedAnalyzer(fieldName).getPositionIncrementGap(fieldName);
    }

    @Override
    public int getOffsetGap(String fieldName) {
        return getWrappedAnalyzer(fieldName).getOffsetGap(fieldName);
    }

    @Override
    public final Reader initReader(String fieldName, Reader reader) {
        return getWrappedAnalyzer(fieldName).initReader(fieldName, reader);
    }
}
