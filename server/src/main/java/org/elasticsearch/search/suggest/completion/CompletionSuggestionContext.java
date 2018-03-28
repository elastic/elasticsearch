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
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CompletionSuggestionContext extends SuggestionSearchContext.SuggestionContext {

    protected CompletionSuggestionContext(QueryShardContext shardContext) {
        super(CompletionSuggester.INSTANCE, shardContext);
    }

    private CompletionFieldMapper.CompletionFieldType fieldType;
    private FuzzyOptions fuzzyOptions;
    private RegexOptions regexOptions;
    private boolean skipDuplicates;
    private Map<String, List<ContextMapping.InternalQueryContext>> queryContexts = Collections.emptyMap();

    CompletionFieldMapper.CompletionFieldType getFieldType() {
        return this.fieldType;
    }

    void setFieldType(CompletionFieldMapper.CompletionFieldType fieldType) {
        this.fieldType = fieldType;
    }

    void setRegexOptions(RegexOptions regexOptions) {
        this.regexOptions = regexOptions;
    }

    void setFuzzyOptions(FuzzyOptions fuzzyOptions) {
        this.fuzzyOptions = fuzzyOptions;
    }

    void setQueryContexts(Map<String, List<ContextMapping.InternalQueryContext>> queryContexts) {
        this.queryContexts = queryContexts;
    }

    void setSkipDuplicates(boolean skipDuplicates) {
        this.skipDuplicates = skipDuplicates;
    }

    public FuzzyOptions getFuzzyOptions() {
        return fuzzyOptions;
    }

    public RegexOptions getRegexOptions() {
        return regexOptions;
    }

    public Map<String, List<ContextMapping.InternalQueryContext>> getQueryContexts() {
        return queryContexts;
    }

    public boolean isSkipDuplicates() {
        return skipDuplicates;
    }

    CompletionQuery toQuery() {
        CompletionFieldMapper.CompletionFieldType fieldType = getFieldType();
        final CompletionQuery query;
        if (getPrefix() != null) {
            query = createCompletionQuery(getPrefix(), fieldType);
        } else if (getRegex() != null) {
            if (fuzzyOptions != null) {
                throw new IllegalArgumentException("can not use 'fuzzy' options with 'regex");
            }
            if (regexOptions == null) {
                regexOptions = RegexOptions.builder().build();
            }
            query = fieldType.regexpQuery(getRegex(), regexOptions.getFlagsValue(),
                    regexOptions.getMaxDeterminizedStates());
        } else if (getText() != null) {
            query = createCompletionQuery(getText(), fieldType);
        } else {
            throw new IllegalArgumentException("'prefix/text' or 'regex' must be defined");
        }
        if (fieldType.hasContextMappings()) {
            ContextMappings contextMappings = fieldType.getContextMappings();
            return contextMappings.toContextQuery(query, queryContexts);
        }
        return query;
    }

    private CompletionQuery createCompletionQuery(BytesRef prefix, CompletionFieldMapper.CompletionFieldType fieldType) {
        final CompletionQuery query;
        if (fuzzyOptions != null) {
            query = fieldType.fuzzyQuery(prefix.utf8ToString(),
                    Fuzziness.fromEdits(fuzzyOptions.getEditDistance()),
                    fuzzyOptions.getFuzzyPrefixLength(), fuzzyOptions.getFuzzyMinLength(),
                    fuzzyOptions.getMaxDeterminizedStates(), fuzzyOptions.isTranspositions(),
                    fuzzyOptions.isUnicodeAware());
        } else {
            query = fieldType.prefixQuery(prefix);
        }
        return query;
    }

}
