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

package org.elasticsearch.index.query.morelikethis;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoostableQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.morelikethis.MoreLikeThisQueryParser.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A more like this query that finds documents that are "like" the provided set of document(s).
 *
 * The documents are provided as a set of strings and/or a list of {@link Item}.
 */
public class MoreLikeThisQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<MoreLikeThisQueryBuilder> {

    // document inputs
    private List<String> likeTexts = new ArrayList<>();
    private List<String> unlikeTexts = new ArrayList<>();
    private List<Item> likeItems = new ArrayList<>();
    private List<Item> unlikeItems = new ArrayList<>();
    private final String[] fields;

    // term selection parameters
    private int maxQueryTerms = -1;
    private int minTermFreq = -1;
    private int minDocFreq = -1;
    private int maxDocFreq = -1;
    private int minWordLength = -1;
    private int maxWordLength = -1;
    private String[] stopWords = null;
    private String analyzer;

    // query formation parameters
    private String minimumShouldMatch = null;
    private float boostTerms = -1;
    private Boolean include = null;

    // other parameters
    private Boolean failOnUnsupportedField;
    private float boost = -1;
    private String queryName;

    /**
     * Constructs a new more like this query which uses the "_all" field.
     */
    public MoreLikeThisQueryBuilder() {
        this.fields = null;
    }

    /**
     * Sets the field names that will be used when generating the 'More Like This' query.
     *
     * @param fields the field names that will be used when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder(String... fields) {
        this.fields = fields;
    }

    /**
     * Sets the text to use in order to find documents that are "like" this.
     *
     * @param likeTexts the text to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder like(String... likeTexts) {
        this.likeTexts = new ArrayList<>();
        return addLikeText(likeTexts);
    }

    /**
     * Sets the documents to use in order to find documents that are "like" this.
     *
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder like(Item... likeItems) {
        this.likeItems = new ArrayList<>();
        return addLikeItem(likeItems);
    }

    /**
     * Adds some text to use in order to find documents that are "like" this.
     */
    public MoreLikeThisQueryBuilder addLikeText(String... likeTexts) {
        Collections.addAll(this.likeTexts, likeTexts);
        return this;
    }

    /**
     * Adds a document to use in order to find documents that are "like" this.
     */
    public MoreLikeThisQueryBuilder addLikeItem(Item... likeItems) {
        Collections.addAll(this.likeItems, likeItems);
        return this;
    }

    /**
     * Sets the text from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder unlike(String... unlikeTexts) {
        this.unlikeTexts = new ArrayList<>();
        return addUnlikeText(unlikeTexts);
    }

    /**
     * Sets the documents from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder unlike(Item... unlikeItems) {
        this.unlikeItems = new ArrayList<>();
        return addUnlikeItem(unlikeItems);
    }

    /**
     * Adds some text to use in order to find documents that are "unlike" this.
     */
    public MoreLikeThisQueryBuilder addUnlikeText(String... unlikeTexts) {
        Collections.addAll(this.unlikeTexts, unlikeTexts);
        return this;
    }

    /**
     * Adds a document to use in order to find documents that are "unlike" this.
     */
    public MoreLikeThisQueryBuilder addUnlikeItem(Item... unlikeItems) {
        Collections.addAll(this.unlikeItems, unlikeItems);
        return this;
    }

    /**
     * Sets the maximum number of query terms that will be included in any generated query.
     * Defaults to <tt>25</tt>.
     */
    public MoreLikeThisQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. The default
     * frequency is <tt>2</tt>.
     */
    public MoreLikeThisQueryBuilder minTermFreq(int minTermFreq) {
        this.minTermFreq = minTermFreq;
        return this;
    }

    /**
     * Sets the frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisQueryBuilder minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Set the maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisQueryBuilder maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    /**
     * Sets the minimum word length below which words will be ignored. Defaults
     * to <tt>0</tt>.
     */
    public MoreLikeThisQueryBuilder minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * Sets the maximum word length above which words will be ignored. Defaults to
     * unbounded (<tt>0</tt>).
     */
    public MoreLikeThisQueryBuilder maxWordLength(int maxWordLength) {
        this.maxWordLength = maxWordLength;
        return this;
    }

    /**
     * Set the set of stopwords.
     * <p/>
     * <p>Any word in this set is considered "uninteresting" and ignored. Even if your Analyzer allows stopwords, you
     * might want to tell the MoreLikeThis code to ignore them, as for the purposes of document similarity it seems
     * reasonable to assume that "a stop word is never interesting".
     */
    public MoreLikeThisQueryBuilder stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the fied.
     */
    public MoreLikeThisQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Number of terms that must match the generated query expressed in the
     * common syntax for minimum should match. Defaults to <tt>30%</tt>.
     *
     * @see    org.elasticsearch.common.lucene.search.Queries#calculateMinShouldMatch(int, String)
     */
    public MoreLikeThisQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Sets the boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisQueryBuilder boostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * Whether to include the input documents. Defaults to <tt>false</tt>
     */
    public MoreLikeThisQueryBuilder include(boolean include) {
        this.include = include;
        return this;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     */
    public MoreLikeThisQueryBuilder failOnUnsupportedField(boolean fail) {
        failOnUnsupportedField = fail;
        return this;
    }

    @Override
    public MoreLikeThisQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public MoreLikeThisQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * The text to use in order to find documents that are "like" this.
     */
    @Deprecated
    public MoreLikeThisQueryBuilder likeText(String likeText) {
        return like(likeText);
    }

    @Deprecated
    public MoreLikeThisQueryBuilder ids(String... ids) {
        Item[] items = new Item[ids.length];
        for (int i = 0; i < items.length; i++) {
            items[i] = new Item(null, null, ids[i]);
        }
        return like(items);
    }

    @Deprecated
    public MoreLikeThisQueryBuilder docs(Item... docs) {
        return like(docs);
    }

    /**
     * Sets the documents from which the terms should not be selected from.
     *
     * @Deprecated Use {@link #unlike(Item...)} instead
     */
    @Deprecated
    public MoreLikeThisQueryBuilder ignoreLike(Item... docs) {
        return unlike(docs);
    }

    /**
     * Sets the text from which the terms should not be selected from.
     *
     * @Deprecated Use {@link #unlike(String...)} instead.
     */
    @Deprecated
    public MoreLikeThisQueryBuilder ignoreLike(String... likeText) {
        return unlike(likeText);
    }

    /**
     * Adds a document to use in order to find documents that are "like" this.
     */
    @Deprecated
    public MoreLikeThisQueryBuilder addItem(Item... likeItems) {
        return addLikeItem(likeItems);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(MoreLikeThisQueryParser.NAME);
        if (fields != null) {
            builder.field(Field.FIELDS.getPreferredName(), fields);
        }
        if (this.likeTexts.isEmpty() && this.likeItems.isEmpty()) {
            throw new IllegalArgumentException("more_like_this requires '" + Field.LIKE.getPreferredName() + "' to be provided");
        } else {
            buildLikeField(builder, Field.LIKE.getPreferredName(), likeTexts, likeItems);
        }
        if (!unlikeTexts.isEmpty() || !unlikeItems.isEmpty()) {
            buildLikeField(builder, Field.UNLIKE.getPreferredName(), unlikeTexts, unlikeItems);
        }
        if (maxQueryTerms != -1) {
            builder.field(Field.MAX_QUERY_TERMS.getPreferredName(), maxQueryTerms);
        }
        if (minTermFreq != -1) {
            builder.field(Field.MIN_TERM_FREQ.getPreferredName(), minTermFreq);
        }
        if (minDocFreq != -1) {
            builder.field(Field.MIN_DOC_FREQ.getPreferredName(), minDocFreq);
        }
        if (maxDocFreq != -1) {
            builder.field(Field.MAX_DOC_FREQ.getPreferredName(), maxDocFreq);
        }
        if (minWordLength != -1) {
            builder.field(Field.MIN_WORD_LENGTH.getPreferredName(), minWordLength);
        }
        if (maxWordLength != -1) {
            builder.field(Field.MAX_WORD_LENGTH.getPreferredName(), maxWordLength);
        }
        if (stopWords != null && stopWords.length > 0) {
            builder.field(Field.STOP_WORDS.getPreferredName(), stopWords);
        }
        if (analyzer != null) {
            builder.field(Field.ANALYZER.getPreferredName(), analyzer);
        }
        if (minimumShouldMatch != null) {
            builder.field(Field.MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        }
        if (boostTerms != -1) {
            builder.field(Field.BOOST_TERMS.getPreferredName(), boostTerms);
        }
        if (include != null) {
            builder.field(Field.INCLUDE.getPreferredName(), include);
        }
        if (failOnUnsupportedField != null) {
            builder.field(Field.FAIL_ON_UNSUPPORTED_FIELD.getPreferredName(), failOnUnsupportedField);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    private static void buildLikeField(XContentBuilder builder, String fieldName, List<String> texts, List<Item> items) throws IOException {
        builder.startArray(fieldName);
        for (String text : texts) {
            builder.value(text);
        }
        for (Item item : items) {
            builder.value(item);
        }
        builder.endArray();
    }
}
