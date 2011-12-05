/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public final class FieldNameAnalyzer extends Analyzer {

    private final ImmutableMap<String, Analyzer> analyzers;

    private final Analyzer defaultAnalyzer;

    public FieldNameAnalyzer(Map<String, Analyzer> analyzers, Analyzer defaultAnalyzer) {
        this.analyzers = ImmutableMap.copyOf(analyzers);
        this.defaultAnalyzer = defaultAnalyzer;
    }

    public ImmutableMap<String, Analyzer> analyzers() {
        return analyzers;
    }

    public Analyzer defaultAnalyzer() {
        return defaultAnalyzer;
    }

    @Override public final TokenStream tokenStream(String fieldName, Reader reader) {
        return getAnalyzer(fieldName).tokenStream(fieldName, reader);
    }

    @Override public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
        return getAnalyzer(fieldName).reusableTokenStream(fieldName, reader);
    }

    @Override public int getPositionIncrementGap(String fieldName) {
        return getAnalyzer(fieldName).getPositionIncrementGap(fieldName);
    }

    @Override public int getOffsetGap(Fieldable field) {
        return getAnalyzer(field.name()).getOffsetGap(field);
    }

    private Analyzer getAnalyzer(String name) {
        Analyzer analyzer = analyzers.get(name);
        if (analyzer != null) {
            return analyzer;
        }
        return defaultAnalyzer;
    }
}