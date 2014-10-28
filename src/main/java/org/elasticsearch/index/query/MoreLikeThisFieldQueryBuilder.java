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

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A more like this query that runs against a specific field.
 */
@Deprecated
public class MoreLikeThisFieldQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<MoreLikeThisFieldQueryBuilder> {

    private final String name;

    private String likeText;
    private float percentTermsToMatch = -1;
    private int minTermFreq = -1;
    private int maxQueryTerms = -1;
    private String[] stopWords = null;
    private int minDocFreq = -1;
    private int maxDocFreq = -1;
    private int minWordLength = -1;
    private int maxWordLength = -1;
    private float boostTerms = -1;
    private float boost = -1;
    private String analyzer;
    private Boolean failOnUnsupportedField;
    private String queryName;

    /**
     * A more like this query that runs against a specific field.
     *
     * @param name The field name to run the query against
     */
    public MoreLikeThisFieldQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * The text to use in order to find documents that are "like" this.
     */
    public MoreLikeThisFieldQueryBuilder likeText(String likeText) {
        this.likeText = likeText;
        return this;
    }

    /**
     * The percentage of terms to match. Defaults to <tt>0.3</tt>.
     */
    public MoreLikeThisFieldQueryBuilder percentTermsToMatch(float percentTermsToMatch) {
        this.percentTermsToMatch = percentTermsToMatch;
        return this;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. The default
     * frequency is <tt>2</tt>.
     */
    public MoreLikeThisFieldQueryBuilder minTermFreq(int minTermFreqy) {
        this.minTermFreq = minTermFreqy;
        return this;
    }

    /**
     * Sets the maximum number of query terms that will be included in any generated query.
     * Defaults to <tt>25</tt>.
     */
    public MoreLikeThisFieldQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * Set the set of stopwords.
     * <p/>
     * <p>Any word in this set is considered "uninteresting" and ignored. Even if your Analyzer allows stopwords, you
     * might want to tell the MoreLikeThis code to ignore them, as for the purposes of document similarity it seems
     * reasonable to assume that "a stop word is never interesting".
     */
    public MoreLikeThisFieldQueryBuilder stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    /**
     * Sets the frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisFieldQueryBuilder minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Set the maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisFieldQueryBuilder maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    /**
     * Sets the minimum word length below which words will be ignored. Defaults
     * to <tt>0</tt>.
     */
    public MoreLikeThisFieldQueryBuilder minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * Sets the maximum word length above which words will be ignored. Defaults to
     * unbounded (<tt>0</tt>).
     */
    public MoreLikeThisFieldQueryBuilder maxWordLen(int maxWordLen) {
        this.maxWordLength = maxWordLen;
        return this;
    }

    /**
     * Sets the boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisFieldQueryBuilder boostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the fied.
     */
    public MoreLikeThisFieldQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public MoreLikeThisFieldQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     */
    public MoreLikeThisFieldQueryBuilder failOnUnsupportedField(boolean fail) {
        failOnUnsupportedField = fail;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public MoreLikeThisFieldQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(MoreLikeThisFieldQueryParser.NAME);
        builder.startObject(name);
        if (likeText == null) {
            throw new ElasticsearchIllegalArgumentException("moreLikeThisField requires '"+
                    MoreLikeThisQueryParser.Fields.LIKE_TEXT.getPreferredName() +"' to be provided");
        }
        builder.field(MoreLikeThisQueryParser.Fields.LIKE_TEXT.getPreferredName(), likeText);
        if (percentTermsToMatch != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.PERCENT_TERMS_TO_MATCH.getPreferredName(), percentTermsToMatch);
        }
        if (minTermFreq != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MIN_TERM_FREQ.getPreferredName(), minTermFreq);
        }
        if (maxQueryTerms != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MAX_QUERY_TERMS.getPreferredName(), maxQueryTerms);
        }
        if (stopWords != null && stopWords.length > 0) {
            builder.startArray(MoreLikeThisQueryParser.Fields.STOP_WORDS.getPreferredName());
            for (String stopWord : stopWords) {
                builder.value(stopWord);
            }
            builder.endArray();
        }
        if (minDocFreq != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MIN_DOC_FREQ.getPreferredName(), minDocFreq);
        }
        if (maxDocFreq != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MAX_DOC_FREQ.getPreferredName(), maxDocFreq);
        }
        if (minWordLength != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MIN_WORD_LENGTH.getPreferredName(), minWordLength);
        }
        if (maxWordLength != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.MAX_WORD_LENGTH.getPreferredName(), maxWordLength);
        }
        if (boostTerms != -1) {
            builder.field(MoreLikeThisQueryParser.Fields.BOOST_TERMS.getPreferredName(), boostTerms);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (failOnUnsupportedField != null) {
            builder.field(MoreLikeThisQueryParser.Fields.FAIL_ON_UNSUPPORTED_FIELD.getPreferredName(), failOnUnsupportedField);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
        builder.endObject();
    }
}
