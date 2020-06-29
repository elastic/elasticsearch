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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/** Base class for {@link MappedFieldType} implementations that use the same
 * representation for internal index terms as the external representation so
 * that partial matching queries such as prefix, wildcard and fuzzy queries
 * can be implemented. */
public abstract class StringFieldType extends TermBasedFieldType {

    private static final Pattern WILDCARD_PATTERN = Pattern.compile("(\\\\.)|([?*]+)");

    public StringFieldType(String name, boolean isSearchable, boolean hasDocValues,
                           TextSearchInfo textSearchInfo, Map<String, String> meta) {
        super(name, isSearchable, hasDocValues, textSearchInfo, meta);
    }

    protected StringFieldType(MappedFieldType ref) {
        super(ref);
    }

    @Override
    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions,
            boolean transpositions, QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException("[fuzzy] queries cannot be executed when '" +
                    ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
        }
        failIfNotIndexed();
        return new FuzzyQuery(new Term(name(), indexedValueForSearch(value)),
                fuzziness.asDistance(BytesRefs.toString(value)), prefixLength, maxExpansions, transpositions);
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException("[prefix] queries cannot be executed when '" +
                    ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false. For optimised prefix queries on text " +
                    "fields please enable [index_prefixes].");
        }
        failIfNotIndexed();
        PrefixQuery query = new PrefixQuery(new Term(name(), indexedValueForSearch(value)));
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    public static final String normalizeWildcardPattern(String fieldname, String value, Analyzer normalizer)  {
        if (normalizer == null) {
            return value;
        }
        // we want to normalize everything except wildcard characters, e.g. F?o Ba* to f?o ba*, even if e.g there
        // is a char_filter that would otherwise remove them
        Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(value);
        BytesRefBuilder sb = new BytesRefBuilder();
        int last = 0;

        while (wildcardMatcher.find()) {
            if (wildcardMatcher.start() > 0) {
                String chunk = value.substring(last, wildcardMatcher.start());

                BytesRef normalized = normalizer.normalize(fieldname, chunk);
                sb.append(normalized);
            }
            // append the matched group - without normalizing
            sb.append(new BytesRef(wildcardMatcher.group()));

            last = wildcardMatcher.end();
        }
        if (last < value.length()) {
            String chunk = value.substring(last);
            BytesRef normalized = normalizer.normalize(fieldname, chunk);
            sb.append(normalized);
        }
        return sb.toBytesRef().utf8ToString();
    }

    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        failIfNotIndexed();
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException("[wildcard] queries cannot be executed when '" +
                    ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
        }

        Term term;
        if (searchAnalyzer() != null) {
            value = normalizeWildcardPattern(name(), value, searchAnalyzer());
            term = new Term(name(), value);
        } else {
            term = new Term(name(), indexedValueForSearch(value));
        }

        WildcardQuery query = new WildcardQuery(term);
        QueryParsers.setRewriteMethod(query, method);
        return query;
    }

    @Override
    public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException("[regexp] queries cannot be executed when '" +
                    ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
        }
        failIfNotIndexed();
        RegexpQuery query = new RegexpQuery(new Term(name(), indexedValueForSearch(value)), flags, maxDeterminizedStates);
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException("[range] queries on [text] or [keyword] fields cannot be executed when '" +
                    ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false.");
        }
        failIfNotIndexed();
        return new TermRangeQuery(name(),
            lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
            upperTerm == null ? null : indexedValueForSearch(upperTerm),
            includeLower, includeUpper);
    }
}
