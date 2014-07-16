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
import org.elasticsearch.common.collect.UpdateInPlaceMap;

/**
 *
 */
public final class FieldNameAnalyzer extends DelegatingAnalyzerWrapper {

    private final UpdateInPlaceMap<String, Analyzer> analyzers;
    private final Analyzer defaultAnalyzer;

    public FieldNameAnalyzer(UpdateInPlaceMap<String, Analyzer> analyzers, Analyzer defaultAnalyzer) {
        super(Analyzer.PER_FIELD_REUSE_STRATEGY);
        this.analyzers = analyzers;
        this.defaultAnalyzer = defaultAnalyzer;
    }

    public UpdateInPlaceMap<String, Analyzer> analyzers() {
        return analyzers;
    }

    public Analyzer defaultAnalyzer() {
        return defaultAnalyzer;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return getAnalyzer(fieldName);
    }

    private Analyzer getAnalyzer(String name) {
        Analyzer analyzer = analyzers.get(name);
        if (analyzer != null) {
            return analyzer;
        }
        return defaultAnalyzer;
    }
}