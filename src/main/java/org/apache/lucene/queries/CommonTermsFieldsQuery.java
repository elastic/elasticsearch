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

package org.apache.lucene.queries;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

import java.util.List;

public class CommonTermsFieldsQuery extends CommonTermsQuery {
    static {
        assert Lucene.VERSION == Version.LUCENE_46 : "LUCENE-5435";
    }

    private final List<String> fields;

    /**
     * Creates a new {@link CommonTermsFieldsQuery}
     * 
     * @param highFreqOccur
     *            {@link Occur} used for high frequency terms
     * @param lowFreqOccur
     *            {@link Occur} used for low frequency terms
     * @param maxTermFrequency
     *            a value in [0..1) (or absolute number >=1) representing the
     *            maximum threshold of a terms document frequency to be
     *            considered a low frequency term.
     * @param fields
     *            fields to match
     * @throws IllegalArgumentException
     *             if {@link Occur#MUST_NOT} is pass as lowFreqOccur or
     *             highFreqOccur
     */
    public CommonTermsFieldsQuery(Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency, List<String> fields) {
        this(highFreqOccur, lowFreqOccur, maxTermFrequency, fields, false);
    }

    /**
     * Creates a new {@link CommonTermsFieldsQuery}
     * 
     * @param highFreqOccur
     *            {@link Occur} used for high frequency terms
     * @param lowFreqOccur
     *            {@link Occur} used for low frequency terms
     * @param maxTermFrequency
     *            a value in [0..1) (or absolute number >=1) representing the
     *            maximum threshold of a terms document frequency to be
     *            considered a low frequency term.
     * @param fields
     *            fields to match
     * @param disableCoord
     *            disables {@link Similarity#coord(int,int)} in scoring for the
     *            low / high frequency sub-queries
     * @throws IllegalArgumentException
     *             if {@link Occur#MUST_NOT} is pass as lowFreqOccur or
     *             highFreqOccur
     */
    public CommonTermsFieldsQuery(Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency, List<String> fields, boolean disableCoord) {
        super(highFreqOccur, lowFreqOccur, maxTermFrequency, disableCoord);

        this.fields = fields;
    }

    /**
     * Get the fields to match.
     */
    public List<String> getFields() {
        return fields;
    }

    protected Query buildQuery(final int maxDoc, final TermContext[] contextArray, final Term[] queryTerms) {
        BooleanQuery lowFreq = new BooleanQuery(disableCoord);
        BooleanQuery highFreq = new BooleanQuery(disableCoord);
        highFreq.setBoost(highFreqBoost);
        lowFreq.setBoost(lowFreqBoost);
        BooleanQuery query = new BooleanQuery(true);
        for (int i = 0; i < queryTerms.length; i++) {
            TermContext termContext = contextArray[i];
            if (termContext == null) {
                lowFreq.add(buildQueryForTerm(queryTerms[i], null), lowFreqOccur);
            } else {
                if ((maxTermFrequency >= 1f && termContext.docFreq() > maxTermFrequency)
                        || (termContext.docFreq() > (int) Math.ceil(maxTermFrequency * (float) maxDoc))) {
                    highFreq.add(buildQueryForTerm(queryTerms[i], termContext), highFreqOccur);
                } else {
                    lowFreq.add(buildQueryForTerm(queryTerms[i], termContext), lowFreqOccur);
                }
            }

        }
        final int numLowFreqClauses = lowFreq.clauses().size();
        final int numHighFreqClauses = highFreq.clauses().size();
        if (lowFreqOccur == Occur.SHOULD && numLowFreqClauses > 0) {
            int minMustMatch = calcLowFreqMinimumNumberShouldMatch(numLowFreqClauses);
            lowFreq.setMinimumNumberShouldMatch(minMustMatch);
        }
        if (highFreqOccur == Occur.SHOULD && numHighFreqClauses > 0) {
            int minMustMatch = calcHighFreqMinimumNumberShouldMatch(numHighFreqClauses);
            highFreq.setMinimumNumberShouldMatch(minMustMatch);
        }
        if (lowFreq.clauses().isEmpty()) {
            /*
             * if lowFreq is empty we rewrite the high freq terms in a
             * conjunction to prevent slow queries.
             */
            if (highFreq.getMinimumNumberShouldMatch() == 0 && highFreqOccur != Occur.MUST) {
                for (BooleanClause booleanClause : highFreq) {
                    booleanClause.setOccur(Occur.MUST);
                }
            }
            highFreq.setBoost(getBoost());
            return highFreq;
        } else if (highFreq.clauses().isEmpty()) {
            // only do low freq terms - we don't have high freq terms
            lowFreq.setBoost(getBoost());
            return lowFreq;
        } else {
            query.add(highFreq, Occur.SHOULD);
            query.add(lowFreq, Occur.MUST);
            query.setBoost(getBoost());
            return query;
        }
    }

    protected Query buildQueryForTerm(Term term, TermContext termContext) {
        if (fields.size() == 1) {
            return buildQueryForSingleField(fields.get(0), term, termContext);
        }
        BooleanQuery query = new BooleanQuery(disableCoord);
        for (String field : fields) {
            query.add(buildQueryForSingleField(field, term, termContext), Occur.SHOULD);
        }
        return query;
    }

    private Query buildQueryForSingleField(String field, Term term, TermContext termContext) {
        if (field.equals(term.field())) {
            if (termContext == null) {
                return new TermQuery(term);
            }
            return new TermQuery(term, termContext);
        }
        return new TermQuery(new Term(field, term.bytes()));
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append('[');
        buffer.append(super.toString(field));
        buffer.append("](for ");
        for (int i = 0; i < fields.size(); i++) {
            buffer.append(fields.get(i));
            if (i != fields.size() - 1)
                buffer.append(", ");
        }
        buffer.append(')');
        return buffer.toString();
    }

}
