/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.elasticsearch.common.io.FastStringReader;

import java.io.IOException;
import java.util.Set;

/**
 *
 */
public class MoreLikeThisQuery extends Query {

    public static final float DEFAULT_PERCENT_TERMS_TO_MATCH = 0.3f;

    private TFIDFSimilarity similarity;

    private String likeText;
    private String[] moreLikeFields;
    private Analyzer analyzer;
    private float percentTermsToMatch = DEFAULT_PERCENT_TERMS_TO_MATCH;
    private int minTermFrequency = MoreLikeThis.DEFAULT_MIN_TERM_FREQ;
    private int maxQueryTerms = MoreLikeThis.DEFAULT_MAX_QUERY_TERMS;
    private Set<?> stopWords = MoreLikeThis.DEFAULT_STOP_WORDS;
    private int minDocFreq = MoreLikeThis.DEFAULT_MIN_DOC_FREQ;
    private int maxDocFreq = MoreLikeThis.DEFAULT_MAX_DOC_FREQ;
    private int minWordLen = MoreLikeThis.DEFAULT_MIN_WORD_LENGTH;
    private int maxWordLen = MoreLikeThis.DEFAULT_MAX_WORD_LENGTH;
    private boolean boostTerms = MoreLikeThis.DEFAULT_BOOST;
    private float boostTermsFactor = 1;


    public MoreLikeThisQuery() {

    }

    public MoreLikeThisQuery(String likeText, String[] moreLikeFields, Analyzer analyzer) {
        this.likeText = likeText;
        this.moreLikeFields = moreLikeFields;
        this.analyzer = analyzer;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        MoreLikeThis mlt = new MoreLikeThis(reader, similarity == null ? new DefaultSimilarity() : similarity);

        mlt.setFieldNames(moreLikeFields);
        mlt.setAnalyzer(analyzer);
        mlt.setMinTermFreq(minTermFrequency);
        mlt.setMinDocFreq(minDocFreq);
        mlt.setMaxDocFreq(maxDocFreq);
        mlt.setMaxQueryTerms(maxQueryTerms);
        mlt.setMinWordLen(minWordLen);
        mlt.setMaxWordLen(maxWordLen);
        mlt.setStopWords(stopWords);
        mlt.setBoost(boostTerms);
        mlt.setBoostFactor(boostTermsFactor);
        //LUCENE 4 UPGRADE this mapps the 3.6 behavior (only use the first field)
        BooleanQuery bq = (BooleanQuery) mlt.like(new FastStringReader(likeText), moreLikeFields[0]);
        BooleanClause[] clauses = bq.getClauses();

        bq.setMinimumNumberShouldMatch((int) (clauses.length * percentTermsToMatch));

        bq.setBoost(getBoost());
        return bq;
    }

    @Override
    public String toString(String field) {
        return "like:" + likeText;
    }

    public String getLikeText() {
        return likeText;
    }

    public void setLikeText(String likeText) {
        this.likeText = likeText;
    }

    public String[] getMoreLikeFields() {
        return moreLikeFields;
    }

    public void setMoreLikeFields(String[] moreLikeFields) {
        this.moreLikeFields = moreLikeFields;
    }

    public Similarity getSimilarity() {
        return similarity;
    }

    public void setSimilarity(Similarity similarity) {
        if (similarity == null || similarity instanceof TFIDFSimilarity) {
            //LUCENE 4 UPGRADE we need TFIDF similarity here so I only set it if it is an instance of it
            this.similarity = (TFIDFSimilarity) similarity;
        }
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public float getPercentTermsToMatch() {
        return percentTermsToMatch;
    }

    public void setPercentTermsToMatch(float percentTermsToMatch) {
        this.percentTermsToMatch = percentTermsToMatch;
    }

    public int getMinTermFrequency() {
        return minTermFrequency;
    }

    public void setMinTermFrequency(int minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
    }

    public int getMaxQueryTerms() {
        return maxQueryTerms;
    }

    public void setMaxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
    }

    public Set<?> getStopWords() {
        return stopWords;
    }

    public void setStopWords(Set<?> stopWords) {
        this.stopWords = stopWords;
    }

    public int getMinDocFreq() {
        return minDocFreq;
    }

    public void setMinDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
    }

    public int getMaxDocFreq() {
        return maxDocFreq;
    }

    public void setMaxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
    }

    public int getMinWordLen() {
        return minWordLen;
    }

    public void setMinWordLen(int minWordLen) {
        this.minWordLen = minWordLen;
    }

    public int getMaxWordLen() {
        return maxWordLen;
    }

    public void setMaxWordLen(int maxWordLen) {
        this.maxWordLen = maxWordLen;
    }

    public boolean isBoostTerms() {
        return boostTerms;
    }

    public void setBoostTerms(boolean boostTerms) {
        this.boostTerms = boostTerms;
    }

    public float getBoostTermsFactor() {
        return boostTermsFactor;
    }

    public void setBoostTermsFactor(float boostTermsFactor) {
        this.boostTermsFactor = boostTermsFactor;
    }
}

