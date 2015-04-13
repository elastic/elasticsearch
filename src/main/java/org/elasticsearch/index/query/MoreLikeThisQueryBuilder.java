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
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * A more like this query that finds documents that are "like" the provided {@link #likeText(String)}
 * which is checked against the fields the query is constructed with.
 */
public class MoreLikeThisQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<MoreLikeThisQueryBuilder> {

    /**
     * A single get item. Pure delegate to multi get.
     */
    public static final class Item extends MultiGetRequest.Item implements ToXContent {
        public static final Item[] EMPTY_ARRAY = new Item[0];

        private BytesReference doc;
        private String likeText;

        public Item() {
            super();
        }

        public Item(String index, @Nullable String type, String id) {
            super(index, type, id);
        }

        public Item(String likeText) {
            this.likeText = likeText;
        }

        public BytesReference doc() {
            return doc;
        }

        public Item doc(XContentBuilder doc) {
            this.doc = doc.bytes();
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (this.likeText != null) {
                return builder.value(this.likeText);
            }
            builder.startObject();
            if (this.index() != null) {
                builder.field("_index", this.index());
            }
            if (this.type() != null) {
                builder.field("_type", this.type());
            }
            if (this.id() != null) {
                builder.field("_id", this.id());
            }
            if (this.doc() != null) {
                XContentType contentType = XContentFactory.xContentType(doc);
                if (contentType == builder.contentType()) {
                    builder.rawField("doc", doc);
                } else {
                    XContentParser parser = XContentFactory.xContent(contentType).createParser(doc);
                    parser.nextToken();
                    builder.field("doc");
                    builder.copyCurrentStructure(parser);
                }
            }
            if (this.fields() != null) {
                builder.array("fields", this.fields());
            }
            if (this.routing() != null) {
                builder.field("_routing", this.routing());
            }
            if (this.fetchSourceContext() != null) {
                FetchSourceContext source = this.fetchSourceContext();
                String[] includes = source.includes();
                String[] excludes = source.excludes();
                if (includes.length == 0 && excludes.length == 0) {
                    builder.field("_source", source.fetchSource());
                } else if (includes.length > 0 && excludes.length == 0) {
                    builder.array("_source", source.includes());
                } else if (excludes.length > 0) {
                    builder.startObject("_source");
                    if (includes.length > 0) {
                        builder.array("includes", source.includes());
                    }
                    builder.array("excludes", source.excludes());
                    builder.endObject();
                }
            }
            if (this.version() != Versions.MATCH_ANY) {
                builder.field("_version", this.version());
            }
            if (this.versionType() != VersionType.INTERNAL) {
                builder.field("_version_type", this.versionType().toString().toLowerCase(Locale.ROOT));
            }
            return builder.endObject();
        }
    }

    private final String[] fields;
    private List<Item> docs = new ArrayList<>();
    private List<Item> ignoreDocs = new ArrayList<>();
    private Boolean include = null;
    private String minimumShouldMatch = null;
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
     * Sets the documents to use in order to find documents that are "like" this.
     *
     * @param docs the documents to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder like(Item... docs) {
        this.docs = Arrays.asList(docs);
        return this;
    }

    /**
     * Sets the text to use in order to find documents that are "like" this.
     *
     * @param likeText the text to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder like(String... likeText) {
        this.docs = new ArrayList<>();
        for (String text : likeText) {
            this.docs.add(new Item(text));
        }
        return this;
    }

    /**
     * Sets the documents from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder ignoreLike(Item... docs) {
        this.ignoreDocs = Arrays.asList(docs);
        return this;
    }

    /**
     * Sets the text from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder ignoreLike(String... likeText) {
        this.ignoreDocs = new ArrayList<>();
        for (String text : likeText) {
            this.ignoreDocs.add(new Item(text));
        }
        return this;
    }

    /**
     * Adds a document to use in order to find documents that are "like" this.
     */
    public MoreLikeThisQueryBuilder addItem(Item item) {
        this.docs.add(item);
        return this;
    }

    /**
     * Adds some text to use in order to find documents that are "like" this.
     */
    public MoreLikeThisQueryBuilder addLikeText(String likeText) {
        this.docs.add(new Item(likeText));
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

    public MoreLikeThisQueryBuilder include(boolean include) {
        this.include = include;
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
     * The percentage of terms to match. Defaults to <tt>0.3</tt>.
     */
    @Deprecated
    public MoreLikeThisQueryBuilder percentTermsToMatch(float percentTermsToMatch) {
        return minimumShouldMatch(Math.round(percentTermsToMatch * 100) + "%");
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
     * Sets the maximum number of query terms that will be included in any generated query.
     * Defaults to <tt>25</tt>.
     */
    public MoreLikeThisQueryBuilder maxQueryTerms(int maxQueryTerms) {
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
    public MoreLikeThisQueryBuilder stopWords(String... stopWords) {
        this.stopWords = stopWords;
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
     * Sets the boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisQueryBuilder boostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the fied.
     */
    public MoreLikeThisQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    @Override
    public MoreLikeThisQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     */
    public MoreLikeThisQueryBuilder failOnUnsupportedField(boolean fail) {
        failOnUnsupportedField = fail;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public MoreLikeThisQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        String likeFieldName = MoreLikeThisQueryParser.Fields.LIKE.getPreferredName();
        builder.startObject(MoreLikeThisQueryParser.NAME);
        if (fields != null) {
            builder.startArray("fields");
            for (String field : fields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (this.docs.isEmpty()) {
            throw new ElasticsearchIllegalArgumentException("more_like_this requires '" + likeFieldName + "' to be provided");
        } else {
            builder.field(likeFieldName, docs);
        }
        if (!ignoreDocs.isEmpty()) {
            builder.field(MoreLikeThisQueryParser.Fields.LIKE.getPreferredName(), ignoreDocs);
        }
        if (minimumShouldMatch != null) {
            builder.field(MoreLikeThisQueryParser.Fields.MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
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
        if (include != null) {
            builder.field("include", include);
        }
        builder.endObject();
    }
}
