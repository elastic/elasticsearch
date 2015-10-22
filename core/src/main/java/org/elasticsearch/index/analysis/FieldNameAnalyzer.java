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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public final class FieldNameAnalyzer extends DelegatingAnalyzerWrapper {

    private final CopyOnWriteHashMap<String, Analyzer> analyzers;
    private final Analyzer defaultAnalyzer;

    public FieldNameAnalyzer(Analyzer defaultAnalyzer) {
        this(new CopyOnWriteHashMap<>(), defaultAnalyzer);
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

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        Analyzer analyzer = analyzers.get(fieldName);
        if (analyzer != null) {
            return analyzer;
        }
        // Don't be lenient here and return the default analyzer
        // Fields need to be explicitly added
        throw new IllegalArgumentException("Field [" + fieldName + "] has no associated analyzer");
    }

    /**
     * Return a new instance that contains the union of this and of the provided analyzers.
     */
    public FieldNameAnalyzer copyAndAddAll(Stream<? extends Map.Entry<String, Analyzer>> mappers) {
        CopyOnWriteHashMap<String, Analyzer> result = analyzers.copyAndPutAll(mappers.map((e) -> {
            if (e.getValue() == null) {
                return new AbstractMap.SimpleImmutableEntry<>(e.getKey(), defaultAnalyzer);
            }
            return e;
        }));
        return new FieldNameAnalyzer(result, defaultAnalyzer);
    }

}
