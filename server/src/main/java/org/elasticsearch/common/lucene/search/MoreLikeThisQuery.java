/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
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
    private String analyzerName;    // used for equals/hashcode
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

    public MoreLikeThisQuery(String likeText, String[] moreLikeFields, NamedAnalyzer analyzer) {
        this.likeText = new String[] { likeText };
        this.moreLikeFields = moreLikeFields;
        this.analyzer = analyzer;
        this.analyzerName = analyzer.name();
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            classHash(),
            boostTerms,
            boostTermsFactor,
            Arrays.hashCode(likeText),
            maxDocFreq,
            maxQueryTerms,
            maxWordLen,
            minDocFreq,
            minTermFrequency,
            minWordLen,
            Arrays.hashCode(moreLikeFields),
            minimumShouldMatch,
            stopWords
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        MoreLikeThisQuery other = (MoreLikeThisQuery) obj;
        if (Objects.equals(analyzerName, other.analyzerName) == false) return false;
        if (boostTerms != other.boostTerms) return false;
        if (boostTermsFactor != other.boostTermsFactor) return false;
        if ((Arrays.equals(likeText, other.likeText)) == false) return false;
        if (maxDocFreq != other.maxDocFreq) return false;
        if (maxQueryTerms != other.maxQueryTerms) return false;
        if (maxWordLen != other.maxWordLen) return false;
        if (minDocFreq != other.minDocFreq) return false;
        if (minTermFrequency != other.minTermFrequency) return false;
        if (minWordLen != other.minWordLen) return false;
        if (Arrays.equals(moreLikeFields, other.moreLikeFields) == false) return false;
        if (minimumShouldMatch.equals(other.minimumShouldMatch) == false) return false;
        if (similarity == null) {
            if (other.similarity != null) return false;
        } else if (similarity.equals(other.similarity) == false) return false;
        if (stopWords == null) {
            if (other.stopWords != null) return false;
        } else if (stopWords.equals(other.stopWords) == false) return false;
        return true;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = super.rewrite(searcher);
        if (rewritten != this) {
            return rewritten;
        }
        XMoreLikeThis mlt = new XMoreLikeThis(searcher.getIndexReader(), similarity == null ? new ClassicSimilarity() : similarity);

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

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
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
            // LUCENE 4 UPGRADE this mapps the 3.6 behavior (only use the first field)
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
        if (skipTerms.isEmpty() == false) {
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

    public void setLikeText(String... likeText) {
        this.likeText = likeText;
    }

    public Fields[] getLikeFields() {
        return likeFields;
    }

    public void setLikeFields(Fields... likeFields) {
        this.likeFields = likeFields;
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

    public void setSimilarity(Similarity similarity) {
        if (similarity == null || similarity instanceof TFIDFSimilarity) {
            // LUCENE 4 UPGRADE we need TFIDF similarity here so I only set it if it is an instance of it
            this.similarity = (TFIDFSimilarity) similarity;
        }
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(String analyzerName, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.analyzerName = analyzerName;
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
        if (maxQueryTerms <= 0) {
            throw new IllegalArgumentException("requires 'maxQueryTerms' to be greater than 0");
        }
        this.maxQueryTerms = maxQueryTerms;
    }

    public void setStopWords(Set<?> stopWords) {
        this.stopWords = stopWords;
    }

    public void setMinDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
    }

    public void setMaxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
    }

    public void setMinWordLen(int minWordLen) {
        this.minWordLen = minWordLen;
    }

    public void setMaxWordLen(int maxWordLen) {
        this.maxWordLen = maxWordLen;
    }

    public void setBoostTerms(boolean boostTerms) {
        this.boostTerms = boostTerms;
    }

    public void setBoostTermsFactor(float boostTermsFactor) {
        this.boostTermsFactor = boostTermsFactor;
    }
}
