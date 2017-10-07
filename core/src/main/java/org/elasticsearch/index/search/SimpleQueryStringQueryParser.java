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
package org.elasticsearch.index.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;

import static org.elasticsearch.common.lucene.search.Queries.newUnmappedFieldQuery;

/**
 * Wrapper class for Lucene's SimpleQueryStringQueryParser that allows us to redefine
 * different types of queries.
 */
public class SimpleQueryStringQueryParser extends SimpleQueryParser {

    private final Settings settings;
    private QueryShardContext context;
    private final MultiMatchQuery queryBuilder;

    /** Creates a new parser with custom flags used to enable/disable certain features. */
    public SimpleQueryStringQueryParser(Map<String, Float> weights, int flags,
                                        Settings settings, QueryShardContext context) {
        this(null, weights, flags, settings, context);
    }

    /** Creates a new parser with custom flags used to enable/disable certain features. */
    public SimpleQueryStringQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags,
                                        Settings settings, QueryShardContext context) {
        super(analyzer, weights, flags);
        this.settings = settings;
        this.context = context;
        this.queryBuilder = new MultiMatchQuery(context);
        this.queryBuilder.setAutoGenerateSynonymsPhraseQuery(settings.autoGenerateSynonymsPhraseQuery());
        this.queryBuilder.setLenient(settings.lenient());
        if (analyzer != null) {
            this.queryBuilder.setAnalyzer(analyzer);
        }
    }

    private Analyzer getAnalyzer(MappedFieldType ft) {
        if (getAnalyzer() != null) {
            return analyzer;
        }
        return ft.searchAnalyzer();
    }

    /**
     * Rethrow the runtime exception, unless the lenient flag has been set, returns null
     */
    private Query rethrowUnlessLenient(RuntimeException e) {
        if (settings.lenient()) {
            return null;
        }
        throw e;
    }

    @Override
    public void setDefaultOperator(BooleanClause.Occur operator) {
        super.setDefaultOperator(operator);
        queryBuilder.setOccur(operator);
    }

    @Override
    protected Query newTermQuery(Term term) {
        MappedFieldType ft = context.fieldMapper(term.field());
        if (ft == null) {
            return newUnmappedFieldQuery(term.field());
        }
        return ft.termQuery(term.bytes(), context);
    }

    @Override
    public Query newDefaultQuery(String text) {
        try {
            return queryBuilder.parse(MultiMatchQueryBuilder.Type.MOST_FIELDS, weights, text, null);
        } catch (IOException e) {
            return rethrowUnlessLenient(new IllegalArgumentException(e.getMessage()));
        }
    }

    @Override
    public Query newFuzzyQuery(String text, int fuzziness) {
        List<Query> disjuncts = new ArrayList<>();
        for (Map.Entry<String,Float> entry : weights.entrySet()) {
            final String fieldName = entry.getKey();
            final MappedFieldType ft = context.fieldMapper(fieldName);
            if (ft == null) {
                disjuncts.add(newUnmappedFieldQuery(fieldName));
                continue;
            }
            try {
                final BytesRef term = getAnalyzer(ft).normalize(fieldName, text);
                Query query = ft.fuzzyQuery(term, Fuzziness.fromEdits(fuzziness), settings.fuzzyPrefixLength,
                    settings.fuzzyMaxExpansions, settings.fuzzyTranspositions);
                disjuncts.add(wrapWithBoost(query, entry.getValue()));
            } catch (RuntimeException e) {
                rethrowUnlessLenient(e);
            }
        }
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }
        return new DisjunctionMaxQuery(disjuncts, 1.0f);
    }

    @Override
    public Query newPhraseQuery(String text, int slop) {
        try {
            queryBuilder.setPhraseSlop(slop);
            Map<String, Float> phraseWeights;
            if (settings.quoteFieldSuffix() != null) {
                phraseWeights = QueryParserHelper.resolveMappingFields(context, weights, settings.quoteFieldSuffix());
            } else {
                phraseWeights = weights;
            }
            return queryBuilder.parse(MultiMatchQueryBuilder.Type.PHRASE, phraseWeights, text, null);
        } catch (IOException e) {
            return rethrowUnlessLenient(new IllegalArgumentException(e.getMessage()));
        } finally {
            queryBuilder.setPhraseSlop(0);
        }
    }

    @Override
    public Query newPrefixQuery(String text) {
        List<Query> disjuncts = new ArrayList<>();
        for (Map.Entry<String,Float> entry : weights.entrySet()) {
            final String fieldName = entry.getKey();
            final MappedFieldType ft = context.fieldMapper(fieldName);
            if (ft == null) {
                disjuncts.add(newUnmappedFieldQuery(fieldName));
                continue;
            }
            try {
                if (settings.analyzeWildcard()) {
                    Query analyzedQuery = newPossiblyAnalyzedQuery(fieldName, text, getAnalyzer(ft));
                    if (analyzedQuery != null) {
                        disjuncts.add(wrapWithBoost(analyzedQuery, entry.getValue()));
                    }
                } else {
                    BytesRef term = getAnalyzer(ft).normalize(fieldName, text);
                    Query query = ft.prefixQuery(term.utf8ToString(), null, context);
                    disjuncts.add(wrapWithBoost(query, entry.getValue()));
                }
            } catch (RuntimeException e) {
                return rethrowUnlessLenient(e);
            }
        }
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }
        return new DisjunctionMaxQuery(disjuncts, 1.0f);
    }

    private static Query wrapWithBoost(Query query, float boost) {
        if (query instanceof MatchNoDocsQuery) {
            return query;
        }
        if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
            return new BoostQuery(query, boost);
        }
        return query;
    }

    /**
     * Analyze the given string using its analyzer, constructing either a
     * {@code PrefixQuery} or a {@code BooleanQuery} made up
     * of {@code TermQuery}s and {@code PrefixQuery}s
     */
    private Query newPossiblyAnalyzedQuery(String field, String termStr, Analyzer analyzer) {
        List<List<BytesRef>> tlist = new ArrayList<> ();
        try (TokenStream source = analyzer.tokenStream(field, termStr)) {
            source.reset();
            List<BytesRef> currentPos = new ArrayList<>();
            CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posAtt = source.addAttribute(PositionIncrementAttribute.class);

            try {
                boolean hasMoreTokens = source.incrementToken();
                while (hasMoreTokens) {
                    if (currentPos.isEmpty() == false && posAtt.getPositionIncrement() > 0) {
                        tlist.add(currentPos);
                        currentPos = new ArrayList<>();
                    }
                    final BytesRef term = analyzer.normalize(field, termAtt.toString());
                    currentPos.add(term);
                    hasMoreTokens = source.incrementToken();
                }
                if (currentPos.isEmpty() == false) {
                    tlist.add(currentPos);
                }
            } catch (IOException e) {
                // ignore
                // TODO: we should not ignore the exception and return a prefix query with the original term ?
            }
        } catch (IOException e) {
            // Bail on any exceptions, going with a regular prefix query
            return new PrefixQuery(new Term(field, termStr));
        }

        if (tlist.size() == 0) {
            return null;
        }

        if (tlist.size() == 1 && tlist.get(0).size() == 1) {
            return new PrefixQuery(new Term(field, tlist.get(0).get(0)));
        }

        // build a boolean query with prefix on the last position only.
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int pos = 0; pos < tlist.size(); pos++) {
            List<BytesRef> plist = tlist.get(pos);
            boolean isLastPos = (pos == tlist.size()-1);
            Query posQuery;
            if (plist.size() == 1) {
                if (isLastPos) {
                    posQuery = new PrefixQuery(new Term(field, plist.get(0)));
                } else {
                    posQuery = newTermQuery(new Term(field, plist.get(0)));
                }
            } else if (isLastPos == false) {
                // build a synonym query for terms in the same position.
                Term[] terms = new Term[plist.size()];
                for (int i = 0; i < plist.size(); i++) {
                    terms[i] = new Term(field, plist.get(i));
                }
                posQuery = new SynonymQuery(terms);
            } else {
                BooleanQuery.Builder innerBuilder = new BooleanQuery.Builder();
                for (BytesRef token : plist) {
                    innerBuilder.add(new BooleanClause(new PrefixQuery(new Term(field, token)),
                        BooleanClause.Occur.SHOULD));
                }
                posQuery = innerBuilder.build();
            }
            builder.add(new BooleanClause(posQuery, getDefaultOperator()));
        }
        return builder.build();
    }

    /**
     * Class encapsulating the settings for the SimpleQueryString query, with
     * their default values
     */
    public static class Settings {
        /** Specifies whether lenient query parsing should be used. */
        private boolean lenient = SimpleQueryStringBuilder.DEFAULT_LENIENT;
        /** Specifies whether wildcards should be analyzed. */
        private boolean analyzeWildcard = SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD;
        /** Specifies a suffix, if any, to apply to field names for phrase matching. */
        private String quoteFieldSuffix = null;
        /** Whether phrase queries should be automatically generated for multi terms synonyms. */
        private boolean autoGenerateSynonymsPhraseQuery = true;
        /** Prefix length in fuzzy queries.*/
        private int fuzzyPrefixLength = SimpleQueryStringBuilder.DEFAULT_FUZZY_PREFIX_LENGTH;
        /** The number of terms fuzzy queries will expand to.*/
        private int fuzzyMaxExpansions = SimpleQueryStringBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS;
        /** Whether transpositions are supported in fuzzy queries.*/
        private boolean fuzzyTranspositions = SimpleQueryStringBuilder.DEFAULT_FUZZY_TRANSPOSITIONS;

        /**
         * Generates default {@link Settings} object (uses ROOT locale, does
         * lowercase terms, no lenient parsing, no wildcard analysis).
         * */
        public Settings() {
        }

        public Settings(Settings other) {
            this.lenient = other.lenient;
            this.analyzeWildcard = other.analyzeWildcard;
            this.quoteFieldSuffix = other.quoteFieldSuffix;
            this.autoGenerateSynonymsPhraseQuery = other.autoGenerateSynonymsPhraseQuery;
            this.fuzzyPrefixLength = other.fuzzyPrefixLength;
            this.fuzzyMaxExpansions = other.fuzzyMaxExpansions;
            this.fuzzyTranspositions = other.fuzzyTranspositions;
        }

        /** Specifies whether to use lenient parsing, defaults to false. */
        public void lenient(boolean lenient) {
            this.lenient = lenient;
        }

        /** Returns whether to use lenient parsing. */
        public boolean lenient() {
            return this.lenient;
        }

        /** Specifies whether to analyze wildcards. Defaults to false if unset. */
        public void analyzeWildcard(boolean analyzeWildcard) {
            this.analyzeWildcard = analyzeWildcard;
        }

        /** Returns whether to analyze wildcards. */
        public boolean analyzeWildcard() {
            return analyzeWildcard;
        }

        /**
         * Set the suffix to append to field names for phrase matching.
         */
        public void quoteFieldSuffix(String suffix) {
            this.quoteFieldSuffix = suffix;
        }

        /**
         * Return the suffix to append for phrase matching, or {@code null} if
         * no suffix should be appended.
         */
        public String quoteFieldSuffix() {
            return quoteFieldSuffix;
        }

        public void autoGenerateSynonymsPhraseQuery(boolean value) {
            this.autoGenerateSynonymsPhraseQuery = value;
        }

        /**
         * Whether phrase queries should be automatically generated for multi terms synonyms.
         * Defaults to <tt>true</tt>.
         */
        public boolean autoGenerateSynonymsPhraseQuery() {
            return autoGenerateSynonymsPhraseQuery;
        }

        public int fuzzyPrefixLength() {
            return fuzzyPrefixLength;
        }

        public void fuzzyPrefixLength(int fuzzyPrefixLength) {
            this.fuzzyPrefixLength = fuzzyPrefixLength;
        }

        public int fuzzyMaxExpansions() {
            return fuzzyMaxExpansions;
        }

        public void fuzzyMaxExpansions(int fuzzyMaxExpansions) {
            this.fuzzyMaxExpansions = fuzzyMaxExpansions;
        }

        public boolean fuzzyTranspositions() {
            return fuzzyTranspositions;
        }

        public void fuzzyTranspositions(boolean fuzzyTranspositions) {
            this.fuzzyTranspositions = fuzzyTranspositions;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lenient, analyzeWildcard, quoteFieldSuffix, autoGenerateSynonymsPhraseQuery,
                fuzzyPrefixLength, fuzzyMaxExpansions, fuzzyTranspositions);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Settings other = (Settings) obj;
            return Objects.equals(lenient, other.lenient) &&
                Objects.equals(analyzeWildcard, other.analyzeWildcard) &&
                Objects.equals(quoteFieldSuffix, other.quoteFieldSuffix) &&
                Objects.equals(autoGenerateSynonymsPhraseQuery, other.autoGenerateSynonymsPhraseQuery) &&
                Objects.equals(fuzzyPrefixLength, fuzzyPrefixLength) &&
                Objects.equals(fuzzyMaxExpansions, fuzzyMaxExpansions) &&
                Objects.equals(fuzzyTranspositions, fuzzyTranspositions);
        }
    }
}
