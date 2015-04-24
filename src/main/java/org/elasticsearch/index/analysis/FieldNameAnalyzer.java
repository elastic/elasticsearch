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
import org.elasticsearch.common.collect.CopyOnWriteHashMap;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public final class FieldNameAnalyzer extends DelegatingAnalyzerWrapper {

    private final CopyOnWriteHashMap<String, Analyzer> analyzers;
    private final Analyzer defaultAnalyzer;

    public FieldNameAnalyzer(Analyzer defaultAnalyzer) {
        this(new CopyOnWriteHashMap<String, Analyzer>(), defaultAnalyzer);
    }

    public FieldNameAnalyzer(Map<String, Analyzer> analyzers, Analyzer defaultAnalyzer) {
        super(Analyzer.PER_FIELD_REUSE_STRATEGY);
        this.analyzers = CopyOnWriteHashMap.copyOf(analyzers);
        this.defaultAnalyzer = defaultAnalyzer;
    }

    public Map<String, Analyzer> analyzers() {
        return analyzers;
    }

    public Analyzer defaultAnalyzer() {
        return defaultAnalyzer;
    }

    /** NOTE: public so MapperAnalyzer can invoke: */
    @Override
    public Analyzer getWrappedAnalyzer(String fieldName) {
        return getAnalyzer(fieldName);
    }

    private Analyzer getAnalyzer(String name) {
        Analyzer analyzer = analyzers.get(name);
        if (analyzer != null) {
            return analyzer;
        }
        return defaultAnalyzer;
    }

    /**
     * Return a new instance that contains the union of this and of the provided analyzers.
     */
    public FieldNameAnalyzer copyAndAddAll(Collection<? extends Map.Entry<String, Analyzer>> mappers) {
        CopyOnWriteHashMap<String, Analyzer> analyzers = this.analyzers;
        for (Map.Entry<String, Analyzer> entry : mappers) {
            if (entry.getValue() != null) {
                analyzers = analyzers.copyAndPut(entry.getKey(), entry.getValue());
            }
        }
        return new FieldNameAnalyzer(analyzers, defaultAnalyzer);
    }

}
