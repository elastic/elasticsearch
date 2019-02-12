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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class MoreLikeThisQuery extends Query {

    public static final String DEFAULT_MINIMUM_SHOULD_MATCH = "30%";

    private TFIDFSimilarity similarity;

    private String[] likeText;
    private Fields[] likeFields;
    private String[] unlikeText;
    private Fields[] unlikeFields;
    private String[] moreLikeFields;
    private Analyzer analyzer;
    private String minimumShouldMatch = DEFAULT_MINIMUM_SHOULD_MATCH;
    private int minTermFrequency = XMoreLikeThis.DEFAULT_MIN_TERM_FREQ;
    private int maxQueryTerms = XMoreLikeThis.DEFAULT_MAX_QUERY_TERMS;
    private Set<?> stopWords = XMoreLikeThis.DEFAULT_STOP_WORDS;
    private int minDocFreq = XMoreLikeThis.DEFAULT_MIN_DOC_FREQ;
    private int maxDocFreq = XMoreLikeThis.DEFAULT_MAX_DOC_FREQ;
    private int minWordLen = XMoreLikeThis.DEFAULT_MIN_WORD_LENGTH;
    private int maxWordLen = XMoreLikeThis.DEFAULT_MAX_WORD_LENGTH;
    private boolean boostTerms = XMoreLikeThis.DEFAULT_BOOST;
    private float boostTermsFactor = 1;


    public MoreLikeThisQuery() {

    }

    public MoreLikeThisQuery(String likeText, String[] moreLikeFields, Analyzer analyzer) {
        this.likeText = new String[]{likeText};
        this.moreLikeFields = moreLikeFields;
        this.analyzer = analyzer;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), boostTerms, boostTermsFactor, Arrays.hashCode(likeText),
                maxDocFreq, maxQueryTerms, maxWordLen, minDocFreq, minTermFrequency, minWordLen,
                Arrays.hashCode(moreLikeFields), minimumShouldMatch, stopWords);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        MoreLikeThisQuery other = (MoreLikeThisQuery) obj;
        if (!analyzer.equals(other.analyzer))
            return false;
        if (boostTerms != other.boostTerms)
            return false;
        if (boostTermsFactor != other.boostTermsFactor)
            return false;
        if (!(Arrays.equals(likeText, other.likeText)))
            return false;
        if (maxDocFreq != other.maxDocFreq)
            return false;
        if (maxQueryTerms != other.maxQueryTerms)
            return false;
        if (maxWordLen != other.maxWordLen)
            return false;
        if (minDocFreq != other.minDocFreq)
            return false;
        if (minTermFrequency != other.minTermFrequency)
            return false;
        if (minWordLen != other.minWordLen)
            return false;
        if (!Arrays.equals(moreLikeFields, other.moreLikeFields))
            return false;
        if (!minimumShouldMatch.equals(other.minimumShouldMatch))
            return false;
        if (similarity == null) {
            if (other.similarity != null)
                return false;
        } else if (!similarity.equals(other.similarity))
            return false;
        if (stopWords == null) {
            if (other.stopWords != null)
                return false;
        } else if (!stopWords.equals(other.stopWords))
            return false;
        return true;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = super.rewrite(reader);
        if (rewritten != this) {
            return rewritten;
        }
        XMoreLikeThis mlt = new XMoreLikeThis(reader, similarity == null ? new ClassicSimilarity() : similarity);

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

        if (this.unlikeText != null || this.unlikeFields != null) {
            handleUnlike(mlt, this.unlikeText, this.unlikeFields);
        }

        return createQuery(mlt);
    }

    private Query createQuery(XMoreLikeThis mlt) throws IOException {
        BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
        if (this.likeFields != null) {
            Query mltQuery = mlt.like(this.likeFields);
            mltQuery = Queries.applyMinimumShouldMatch((BooleanQuery) mltQuery, minimumShouldMatch);
            bqBuilder.add(mltQuery, BooleanClause.Occur.SHOULD);
        }
        if (this.likeText != null) {
            Reader[] readers = new Reader[likeText.length];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = new StringReader(likeText[i]);
            }
            //LUCENE 4 UPGRADE this mapps the 3.6 behavior (only use the first field)
            Query mltQuery = mlt.like(moreLikeFields[0], readers);
            mltQuery = Queries.applyMinimumShouldMatch((BooleanQuery) mltQuery, minimumShouldMatch);
            bqBuilder.add(mltQuery, BooleanClause.Occur.SHOULD);
        }
        return bqBuilder.build();
    }

    private void handleUnlike(XMoreLikeThis mlt, String[] unlikeText, Fields[] unlikeFields) throws IOException {
        Set<Term> skipTerms = new HashSet<>();
        // handle like text
        if (unlikeText != null) {
            for (String text : unlikeText) {
                // only use the first field to be consistent
                String fieldName = moreLikeFields[0];
                try (TokenStream ts = analyzer.tokenStream(fieldName, text)) {
                    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
                    ts.reset();
                    while (ts.incrementToken()) {
                        skipTerms.add(new Term(fieldName, termAtt.toString()));
                    }
                    ts.end();
                }
            }
        }
        // handle like fields
        if (unlikeFields != null) {
            for (Fields fields : unlikeFields) {
                for (String fieldName : fields) {
                    Terms terms = fields.terms(fieldName);
                    final TermsEnum termsEnum = terms.iterator();
                    BytesRef text;
                    while ((text = termsEnum.next()) != null) {
                        skipTerms.add(new Term(fieldName, text.utf8ToString()));
                    }
                }
            }
        }
        if (!skipTerms.isEmpty()) {
            mlt.setSkipTerms(skipTerms);
        }
    }

    @Override
    public String toString(String field) {
        return "like:" + Arrays.toString(likeText);
    }

    public String getLikeText() {
        return (likeText == null ? null : likeText[0]);
    }

    public String[] getLikeTexts() {
        return likeText;
    }

    public void setLikeText(String... likeText) {
        this.likeText = likeText;
    }

    public Fields[] getLikeFields() {
        return likeFields;
    }

    public void setLikeFields(Fields... likeFields) {
        this.likeFields = likeFields;
    }

    public void setLikeText(List<String> likeText) {
        setLikeText(likeText.toArray(Strings.EMPTY_ARRAY));
    }

    public void setUnlikeFields(Fields... unlikeFields) {
        this.unlikeFields = unlikeFields;
    }

    public void setUnlikeText(String[] unlikeText) {
        this.unlikeText = unlikeText;
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

    /**
     * Number of terms that must match the generated query expressed in the
     * common syntax for minimum should match.
     *
     * @see    org.elasticsearch.common.lucene.search.Queries#calculateMinShouldMatch(int, String)
     */
    public String getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    /**
     * Number of terms that must match the generated query expressed in the
     * common syntax for minimum should match. Defaults to {@code 30%}.
     *
     * @see    org.elasticsearch.common.lucene.search.Queries#calculateMinShouldMatch(int, String)
     */
    public void setMinimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
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
