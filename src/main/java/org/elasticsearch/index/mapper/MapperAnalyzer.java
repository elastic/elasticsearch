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

package org.elasticsearch.index.mapper;

import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

/** Hacky analyzer to dispatch per-thread based on the type of the current document being indexed, to look up the per-field Analyzer.  Once
 *  mappings are moved to the index level we can remove this. */
public class MapperAnalyzer extends DelegatingAnalyzerWrapper {

    private final MapperService mapperService;

    /** Which type this thread is currently indexing. */
    private final ThreadLocal<String> threadTypes = new ThreadLocal<>();

    public MapperAnalyzer(MapperService mapperService) {
        super(Analyzer.PER_FIELD_REUSE_STRATEGY);
        this.mapperService = mapperService;
    }

    /** Any thread that is about to use this analyzer for indexing must first set the type by calling this. */
    public void setType(String type) {
        threadTypes.set(type);
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        // First get the FieldNameAnalyzer from the type, then ask it for the right analyzer for this field, or the default index analyzer:
        return ((FieldNameAnalyzer) mapperService.documentMapper(threadTypes.get()).mappers().indexAnalyzer()).getWrappedAnalyzer(fieldName);
    }
}
