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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Base class for {@link MappedFieldType} implementations that use the same
 * representation for internal index terms as the external representation so
 * that partial matching queries such as prefix, wildcard and fuzzy queries
 * can be implemented. */
public abstract class StringFieldType extends TermBasedFieldType {

    public StringFieldType() {}

    protected StringFieldType(MappedFieldType ref) {
        super(ref);
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        failIfNotIndexed();
        BytesRef[] bytesRefs = new BytesRef[values.size()];
        for (int i = 0; i < bytesRefs.length; i++) {
            bytesRefs[i] = indexedValueForSearch(values.get(i));
        }
        return new TermInSetQuery(name(), bytesRefs);
    }

    @Override
    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions,
            boolean transpositions) {
        failIfNotIndexed();
        return new FuzzyQuery(new Term(name(), indexedValueForSearch(value)),
                fuzziness.asDistance(BytesRefs.toString(value)), prefixLength, maxExpansions, transpositions);
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        failIfNotIndexed();
        PrefixQuery query = new PrefixQuery(new Term(name(), indexedValueForSearch(value)));
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        Query termQuery = termQuery(value, context);
        if (termQuery instanceof MatchNoDocsQuery || termQuery instanceof MatchAllDocsQuery) {
            return termQuery;
        }
        Term term = MappedFieldType.extractTerm(termQuery);

        WildcardQuery query = new WildcardQuery(term);
        QueryParsers.setRewriteMethod(query, method);
        return query;
    }

    @Override
    public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        failIfNotIndexed();
        RegexpQuery query = new RegexpQuery(new Term(name(), indexedValueForSearch(value)), flags, maxDeterminizedStates);
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
        failIfNotIndexed();
        return new TermRangeQuery(name(),
            lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
            upperTerm == null ? null : indexedValueForSearch(upperTerm),
            includeLower, includeUpper);
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements) throws IOException {

        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
        int position = -1;

        stream.reset();
        while (stream.incrementToken()) {
            if (enablePosIncrements) {
                position += posIncrAtt.getPositionIncrement();
            }
            else {
                position += 1;
            }
            builder.add(new Term(name(), termAtt.getBytesRef()), position);
        }

        return builder.build();
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        return createPhraseQuery(stream, name(), slop, enablePositionIncrements);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
        return createPhrasePrefixQuery(stream, name(), slop, maxExpansions);
    }

    static Query createPhraseQuery(TokenStream stream, String field, int slop, boolean enablePositionIncrements) throws IOException {
        MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();
        mpqb.setSlop(slop);

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
        int position = -1;

        List<Term> multiTerms = new ArrayList<>();
        stream.reset();
        while (stream.incrementToken()) {
            int positionIncrement = posIncrAtt.getPositionIncrement();

            if (positionIncrement > 0 && multiTerms.size() > 0) {
                if (enablePositionIncrements) {
                    mpqb.add(multiTerms.toArray(new Term[0]), position);
                } else {
                    mpqb.add(multiTerms.toArray(new Term[0]));
                }
                multiTerms.clear();
            }
            position += positionIncrement;
            multiTerms.add(new Term(field, termAtt.getBytesRef()));
        }

        if (enablePositionIncrements) {
            mpqb.add(multiTerms.toArray(new Term[0]), position);
        } else {
            mpqb.add(multiTerms.toArray(new Term[0]));
        }
        return mpqb.build();
    }

    static MultiPhrasePrefixQuery createPhrasePrefixQuery(TokenStream stream, String field,
                                                          int slop, int maxExpansions) throws IOException {
        MultiPhrasePrefixQuery builder = new MultiPhrasePrefixQuery(field);
        builder.setSlop(slop);
        builder.setMaxExpansions(maxExpansions);

        List<Term> currentTerms = new ArrayList<>();

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);

        stream.reset();
        int position = -1;
        while (stream.incrementToken()) {
            if (posIncrAtt.getPositionIncrement() != 0) {
                if (currentTerms.isEmpty() == false) {
                    builder.add(currentTerms.toArray(new Term[0]), position);
                }
                position += posIncrAtt.getPositionIncrement();
                currentTerms.clear();
            }
            currentTerms.add(new Term(field, termAtt.getBytesRef()));
        }
        builder.add(currentTerms.toArray(new Term[0]), position);
        return builder;
    }

}
