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
package org.elasticsearch.search.suggest.filteredsuggest;

import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.ContextQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FilteredSuggestFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.completion.FuzzyOptions;
import org.elasticsearch.search.suggest.completion.RegexOptions;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.filteredsuggest.filter.FilterMappings;

import java.util.List;
import java.util.Map;

public class FilteredSuggestSuggestionContext extends SuggestionSearchContext.SuggestionContext {

    protected FilteredSuggestSuggestionContext(QueryShardContext shardContext) {
        super(FilteredSuggestSuggester.INSTANCE, shardContext);
    }

    private FilteredSuggestFieldMapper.CompletionFieldType fieldType;
    private FuzzyOptions fuzzyOptions;
    private RegexOptions regexOptions;
    private Map<String, List<ContextMapping.InternalQueryContext>> queryFilters;

    FilteredSuggestFieldMapper.CompletionFieldType getFieldType() {
        return this.fieldType;
    }

    void setFieldType(FilteredSuggestFieldMapper.CompletionFieldType fieldType) {
        this.fieldType = fieldType;
    }

    void setFuzzyOptions(FuzzyOptions fuzzyOptions) {
        this.fuzzyOptions = fuzzyOptions;
    }

    void setRegexOptions(RegexOptions regexOptions) {
        this.regexOptions = regexOptions;
    }

    public void setQueryFilters(Map<String, List<ContextMapping.InternalQueryContext>> queryFilters) {
        this.queryFilters = queryFilters;
    }

    public FuzzyOptions getFuzzyOptions() {
        return fuzzyOptions;
    }

    public RegexOptions getRegexOptions() {
        return regexOptions;
    }

    public Map<String, List<ContextMapping.InternalQueryContext>> getQueryFilters() {
        return queryFilters;
    }

    List<ContextQuery> toQueries() {
        FilteredSuggestFieldMapper.CompletionFieldType fieldType = getFieldType();
        final CompletionQuery query;
        if (getPrefix() != null) {
            if (fuzzyOptions != null) {
                query = fieldType.fuzzyQuery(getPrefix().utf8ToString(), Fuzziness.fromEdits(fuzzyOptions.getEditDistance()),
                        fuzzyOptions.getFuzzyPrefixLength(), fuzzyOptions.getFuzzyMinLength(), fuzzyOptions.getMaxDeterminizedStates(),
                        fuzzyOptions.isTranspositions(), fuzzyOptions.isUnicodeAware());
            } else {
                query = fieldType.prefixQuery(getPrefix());
            }
        } else if (getRegex() != null) {
            if (fuzzyOptions != null) {
                throw new IllegalArgumentException("can not use 'fuzzy' options with 'regex");
            }
            if (regexOptions == null) {
                regexOptions = RegexOptions.builder().build();
            }
            query = fieldType.regexpQuery(getRegex(), regexOptions.getFlagsValue(), regexOptions.getMaxDeterminizedStates());
        } else {
            throw new IllegalArgumentException("'prefix' or 'regex' must be defined");
        }

        FilterMappings filterMappings = fieldType.getFilterMappings();
        return filterMappings.toContextQuery(query, queryFilters);
    }
}