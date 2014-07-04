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

package org.apache.lucene.analysis;

import java.io.Reader;

/**
 * A simple analyzer wrapper, that doesn't allow to wrap components or reader. By disallowing
 * it, it means that the thread local resources will be delegated to the wrapped analyzer, and not
 * also be allocated on this analyzer.
 *
 * This solves the problem of per field analyzer wrapper, where it also maintains a thread local
 * per field token stream components, while it can safely delegate those and not also hold these
 * data structures, which can become expensive memory wise.
 */
public abstract class SimpleAnalyzerWrapper extends AnalyzerWrapper {

    static {
        assert org.elasticsearch.Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9: "Remove this code once we upgrade to Lucene 4.10 (LUCENE-5803)";
    }

    public SimpleAnalyzerWrapper() {
        super(new DelegatingReuseStrategy());
        ((DelegatingReuseStrategy) getReuseStrategy()).wrapper = this;
    }

    @Override
    protected final TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return super.wrapComponents(fieldName, components);
    }

    @Override
    protected final Reader wrapReader(String fieldName, Reader reader) {
        return super.wrapReader(fieldName, reader);
    }

    private static class DelegatingReuseStrategy extends ReuseStrategy {

        AnalyzerWrapper wrapper;

        @Override
        public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            Analyzer wrappedAnalyzer = wrapper.getWrappedAnalyzer(fieldName);
            return wrappedAnalyzer.getReuseStrategy().getReusableComponents(wrappedAnalyzer, fieldName);
        }

        @Override
        public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents components) {
            Analyzer wrappedAnalyzer = wrapper.getWrappedAnalyzer(fieldName);
            wrappedAnalyzer.getReuseStrategy().setReusableComponents(wrappedAnalyzer, fieldName, components);
        }
    }
}
