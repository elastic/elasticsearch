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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.elasticsearch.common.collect.ImmutableOpenMap;

/**
 *
 */
public final class FieldNameAnalyzer extends AnalyzerWrapper {

    private final ImmutableOpenMap<String, Analyzer> analyzers;

    private final Analyzer defaultAnalyzer;

    public FieldNameAnalyzer(ImmutableOpenMap<String, Analyzer> analyzers, Analyzer defaultAnalyzer) {
        this.analyzers = analyzers;
        this.defaultAnalyzer = defaultAnalyzer;
    }

    public ImmutableOpenMap<String, Analyzer> analyzers() {
        return analyzers;
    }

    public Analyzer defaultAnalyzer() {
        return defaultAnalyzer;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return getAnalyzer(fieldName);
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return components;
    }

    private Analyzer getAnalyzer(String name) {
        Analyzer analyzer = analyzers.get(name);
        if (analyzer != null) {
            return analyzer;
        }
        return defaultAnalyzer;
    }
}