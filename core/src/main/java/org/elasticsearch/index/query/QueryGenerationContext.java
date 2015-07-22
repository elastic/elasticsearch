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

package org.elasticsearch.index.query;

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.util.Collection;

/**
 * Wrapper around a {@link QueryParseContext} that can be later on fleshed out to only
 * include methods and state that we need in the query generation phase
 */
public class QueryGenerationContext {

    private final QueryParseContext parseContext;

    public QueryGenerationContext(QueryParseContext parseContext) {
        this.parseContext = parseContext;
    }

    public void addNamedQuery(String queryName, Query query) {
        this.parseContext.addNamedQuery(queryName, query);
    }

    /**
     * @return
     */
    //norelease temporary way of still accessing the wrapped parse context
    public QueryParseContext getParseContext() {
        return this.parseContext;
    }

    public IndexQueryParserService indexQueryParserService() {
        return this.parseContext.indexQueryParserService();
    }

    public MapperService mapperService() {
        return this.parseContext.mapperService();
    }

    public Analyzer getSearchAnalyzer(MappedFieldType fieldType) {
        return this.parseContext.getSearchAnalyzer(fieldType);
    }

    public Collection<String> queryTypes() {
        return this.parseContext.queryTypes();
    }

    public MappedFieldType fieldMapper(String fieldName) {
        return this.parseContext.fieldMapper(fieldName);
    }

    public String defaultField() {
        return this.parseContext.defaultField();
    }

    public AnalysisService analysisService() {
        return this.parseContext.analysisService();
    }

    public ObjectMapper getObjectMapper(String fieldPattern) {
        return this.parseContext.getObjectMapper(fieldPattern);
    }

    public Collection<String> simpleMatchToIndexNames(String fieldPattern) {
        return this.parseContext.simpleMatchToIndexNames(fieldPattern);
    }

    public void setAllowUnmappedFields(boolean flag) {
        this.parseContext.setAllowUnmappedFields(flag);
    }

    public Index index() {
        return this.parseContext.index();
    }

    public ImmutableMap<String, Query> copyNamedQueries() {
        return this.parseContext.copyNamedQueries();
    }
}
