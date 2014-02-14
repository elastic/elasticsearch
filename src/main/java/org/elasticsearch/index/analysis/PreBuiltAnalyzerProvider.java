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

/**
 *
 */
public class PreBuiltAnalyzerProvider implements AnalyzerProvider<NamedAnalyzer> {

    private final NamedAnalyzer analyzer;

    public PreBuiltAnalyzerProvider(String name, AnalyzerScope scope, Analyzer analyzer) {
        // we create the named analyzer here so the resources associated with it will be shared
        // and we won't wrap a shared analyzer with named analyzer each time causing the resources
        // to not be shared...
        this.analyzer = new NamedAnalyzer(name, scope, analyzer);
    }

    @Override
    public String name() {
        return analyzer.name();
    }

    @Override
    public AnalyzerScope scope() {
        return analyzer.scope();
    }

    @Override
    public NamedAnalyzer get() {
        return analyzer;
    }
}
