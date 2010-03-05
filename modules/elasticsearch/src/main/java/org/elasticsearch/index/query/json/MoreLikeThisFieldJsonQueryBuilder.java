/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.json;

import org.elasticsearch.index.query.QueryBuilderException;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * A more like this query that runs against a specific field.
 *
 * @author kimchy (shay.banon)
 */
public class MoreLikeThisFieldJsonQueryBuilder extends BaseJsonQueryBuilder {

    private final String name;

    private String likeText;
    private float percentTermsToMatch = -1;
    private int minTermFrequency = -1;
    private int maxQueryTerms = -1;
    private String[] stopWords = null;
    private int minDocFreq = -1;
    private int maxDocFreq = -1;
    private int minWordLen = -1;
    private int maxWordLen = -1;
    private Boolean boostTerms = null;
    private float boostTermsFactor = -1;

    /**
     * A more like this query that runs against a specific field.
     *
     * @param name The field name to run the query against
     */
    public MoreLikeThisFieldJsonQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * The text to use in order to find documents that are "like" this.
     */
    public MoreLikeThisFieldJsonQueryBuilder likeText(String likeText) {
        this.likeText = likeText;
        return this;
    }

    /**
     * The percentage of terms to match. Defaults to <tt>0.3</tt>.
     */
    public MoreLikeThisFieldJsonQueryBuilder percentTermsToMatch(float percentTermsToMatch) {
        this.percentTermsToMatch = percentTermsToMatch;
        return this;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. The default
     * frequency is <tt>2</tt>.
     */
    public MoreLikeThisFieldJsonQueryBuilder minTermFrequency(int minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
        return this;
    }

    /**
     * Sets the maximum number of query terms that will be included in any generated query.
     * Defaults to <tt>25</tt>.
     */
    public MoreLikeThisFieldJsonQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * Set the set of stopwords.
     *
     * <p>Any word in this set is considered "uninteresting" and ignored. Even if your Analyzer allows stopwords, you
     * might want to tell the MoreLikeThis code to ignore them, as for the purposes of document similarity it seems
     * reasonable to assume that "a stop word is never interesting".
     */
    public MoreLikeThisFieldJsonQueryBuilder stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    /**
     * Sets the frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisFieldJsonQueryBuilder minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Set the maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisFieldJsonQueryBuilder maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    /**
     * Sets the minimum word length below which words will be ignored. Defaults
     * to <tt>0</tt>.
     */
    public MoreLikeThisFieldJsonQueryBuilder minWordLen(int minWordLen) {
        this.minWordLen = minWordLen;
        return this;
    }

    /**
     * Sets the maximum word length above which words will be ignored. Defaults to
     * unbounded (<tt>0</tt>).
     */
    public MoreLikeThisFieldJsonQueryBuilder maxWordLen(int maxWordLen) {
        this.maxWordLen = maxWordLen;
        return this;
    }

    /**
     * Sets whether to boost terms in query based on "score" or not. Defaults to
     * <tt>false</tt>.
     */
    public MoreLikeThisFieldJsonQueryBuilder boostTerms(Boolean boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * Sets the boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisFieldJsonQueryBuilder boostTermsFactor(float boostTermsFactor) {
        this.boostTermsFactor = boostTermsFactor;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject(MoreLikeThisFieldJsonQueryParser.NAME);
        builder.startObject(name);
        if (likeText == null) {
            throw new QueryBuilderException("moreLikeThisField requires 'likeText' to be provided");
        }
        builder.field("likeText", likeText);
        if (percentTermsToMatch != -1) {
            builder.field("percentTermsToMatch", percentTermsToMatch);
        }
        if (minTermFrequency != -1) {
            builder.field("minTermFrequency", minTermFrequency);
        }
        if (maxQueryTerms != -1) {
            builder.field("maxQueryTerms", maxQueryTerms);
        }
        if (stopWords != null && stopWords.length > 0) {
            builder.startArray("stopWords");
            for (String stopWord : stopWords) {
                builder.value(stopWord);
            }
            builder.endArray();
        }
        if (minDocFreq != -1) {
            builder.field("minDocFreq", minDocFreq);
        }
        if (maxDocFreq != -1) {
            builder.field("maxDocFreq", maxDocFreq);
        }
        if (minWordLen != -1) {
            builder.field("minWordLen", minWordLen);
        }
        if (maxWordLen != -1) {
            builder.field("maxWordLen", maxWordLen);
        }
        if (boostTerms != null) {
            builder.field("boostTerms", boostTerms);
        }
        if (boostTermsFactor != -1) {
            builder.field("boostTermsFactor", boostTermsFactor);
        }
        builder.endObject();
        builder.endObject();
    }
}