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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.index.mapper.Uid.createUidAsBytes;

/**
 * A more like this query that finds documents that are "like" the provided {@link #likeText(String)}
 * which is checked against the fields the query is constructed with.
 */
public class MoreLikeThisQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<MoreLikeThisQueryBuilder> {

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

    public static final String NAME = "mlt";
    private MoreLikeThisFetchService fetchService = null;

    public static class Fields {
        public static final ParseField LIKE_TEXT = new ParseField("like_text").withAllDeprecated("like");
        public static final ParseField MIN_TERM_FREQ = new ParseField("min_term_freq");
        public static final ParseField MAX_QUERY_TERMS = new ParseField("max_query_terms");
        public static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        public static final ParseField MAX_WORD_LENGTH = new ParseField("max_word_length", "max_word_len");
        public static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        public static final ParseField MAX_DOC_FREQ = new ParseField("max_doc_freq");
        public static final ParseField BOOST_TERMS = new ParseField("boost_terms");
        public static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
        public static final ParseField PERCENT_TERMS_TO_MATCH = new ParseField("percent_terms_to_match").withAllDeprecated("minimum_should_match");
        public static final ParseField FAIL_ON_UNSUPPORTED_FIELD = new ParseField("fail_on_unsupported_field");
        public static final ParseField STOP_WORDS = new ParseField("stop_words");
        public static final ParseField DOCUMENT_IDS = new ParseField("ids").withAllDeprecated("like");
        public static final ParseField DOCUMENTS = new ParseField("docs").withAllDeprecated("like");
        public static final ParseField LIKE = new ParseField("like");
        public static final ParseField IGNORE_LIKE = new ParseField("ignore_like");
        public static final ParseField INCLUDE = new ParseField("include");
    }

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
        String likeFieldName = MoreLikeThisQueryBuilder.Fields.LIKE.getPreferredName();
        builder.startObject(MoreLikeThisQueryBuilder.NAME);
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
            builder.field(MoreLikeThisQueryBuilder.Fields.LIKE.getPreferredName(), ignoreDocs);
        }
        if (minimumShouldMatch != null) {
            builder.field(MoreLikeThisQueryBuilder.Fields.MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        }
        if (minTermFreq != -1) {
            builder.field(MoreLikeThisQueryBuilder.Fields.MIN_TERM_FREQ.getPreferredName(), minTermFreq);
        }
        if (maxQueryTerms != -1) {
            builder.field(MoreLikeThisQueryBuilder.Fields.MAX_QUERY_TERMS.getPreferredName(), maxQueryTerms);
        }
        if (stopWords != null && stopWords.length > 0) {
            builder.startArray(MoreLikeThisQueryBuilder.Fields.STOP_WORDS.getPreferredName());
            for (String stopWord : stopWords) {
                builder.value(stopWord);
            }
            builder.endArray();
        }
        if (minDocFreq != -1) {
            builder.field(MoreLikeThisQueryBuilder.Fields.MIN_DOC_FREQ.getPreferredName(), minDocFreq);
        }
        if (maxDocFreq != -1) {
            builder.field(MoreLikeThisQueryBuilder.Fields.MAX_DOC_FREQ.getPreferredName(), maxDocFreq);
        }
        if (minWordLength != -1) {
            builder.field(MoreLikeThisQueryBuilder.Fields.MIN_WORD_LENGTH.getPreferredName(), minWordLength);
        }
        if (maxWordLength != -1) {
            builder.field(MoreLikeThisQueryBuilder.Fields.MAX_WORD_LENGTH.getPreferredName(), maxWordLength);
        }
        if (boostTerms != -1) {
            builder.field(MoreLikeThisQueryBuilder.Fields.BOOST_TERMS.getPreferredName(), boostTerms);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (failOnUnsupportedField != null) {
            builder.field(MoreLikeThisQueryBuilder.Fields.FAIL_ON_UNSUPPORTED_FIELD.getPreferredName(), failOnUnsupportedField);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        if (include != null) {
            builder.field("include", include);
        }
        builder.endObject();
    }

    @Inject(optional = true)
    public void setFetchService(@Nullable MoreLikeThisFetchService fetchService) {
        this.fetchService = fetchService;
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "more_like_this", "moreLikeThis"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();
        mltQuery.setSimilarity(parseContext.searchSimilarity());
        Analyzer analyzer = null;
        List<String> moreLikeFields = null;
        boolean failOnUnsupportedField = true;
        String queryName = null;
        boolean include = false;

        XContentParser.Token token;
        String currentFieldName = null;

        List<String> likeTexts = new ArrayList<>();
        MultiTermVectorsRequest likeItems = new MultiTermVectorsRequest();

        List<String> ignoreTexts = new ArrayList<>();
        MultiTermVectorsRequest ignoreItems = new MultiTermVectorsRequest();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.LIKE_TEXT.match(currentFieldName, parseContext.parseFlags())) {
                    likeTexts.add(parser.text());
                } else if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, likeTexts, likeItems);
                } else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, ignoreTexts, ignoreItems);
                } else if (Fields.MIN_TERM_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinTermFrequency(parser.intValue());
                } else if (Fields.MAX_QUERY_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxQueryTerms(parser.intValue());
                } else if (Fields.MIN_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinDocFreq(parser.intValue());
                } else if (Fields.MAX_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxDocFreq(parser.intValue());
                } else if (Fields.MIN_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinWordLen(parser.intValue());
                } else if (Fields.MAX_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxWordLen(parser.intValue());
                } else if (Fields.BOOST_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    float boostFactor = parser.floatValue();
                    if (boostFactor != 0) {
                        mltQuery.setBoostTerms(true);
                        mltQuery.setBoostTermsFactor(boostFactor);
                    }
                } else if (Fields.MINIMUM_SHOULD_MATCH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinimumShouldMatch(parser.text());
                } else if (Fields.PERCENT_TERMS_TO_MATCH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinimumShouldMatch(Math.round(parser.floatValue() * 100) + "%");
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(parser.floatValue());
                } else if (Fields.FAIL_ON_UNSUPPORTED_FIELD.match(currentFieldName, parseContext.parseFlags())) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if (Fields.INCLUDE.match(currentFieldName, parseContext.parseFlags())) {
                    include = parser.booleanValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Fields.STOP_WORDS.match(currentFieldName, parseContext.parseFlags())) {
                    Set<String> stopWords = Sets.newHashSet();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                    mltQuery.setStopWords(stopWords);
                } else if ("fields".equals(currentFieldName)) {
                    moreLikeFields = Lists.newLinkedList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        moreLikeFields.add(parseContext.indexName(parser.text()));
                    }
                } else if (Fields.DOCUMENT_IDS.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (!token.isValue()) {
                            throw new ElasticsearchIllegalArgumentException("ids array element should only contain ids");
                        }
                        likeItems.add(newTermVectorsRequest().id(parser.text()));
                    }
                } else if (Fields.DOCUMENTS.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchIllegalArgumentException("docs array element should include an object");
                        }
                        likeItems.add(parseDocument(parser));
                    }
                } else if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, likeTexts, likeItems);
                    }
                } else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, ignoreTexts, ignoreItems);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, likeTexts, likeItems);
                }
                else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, ignoreTexts, ignoreItems);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (likeTexts.isEmpty() && likeItems.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires 'like' to be specified");
        }
        if (moreLikeFields != null && moreLikeFields.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires 'fields' to be non-empty");
        }

        // set analyzer
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }
        mltQuery.setAnalyzer(analyzer);

        // set like text fields
        boolean useDefaultField = (moreLikeFields == null);
        if (useDefaultField) {
            moreLikeFields = Lists.newArrayList(parseContext.defaultField());
        }
        // possibly remove unsupported fields
        removeUnsupportedFields(moreLikeFields, analyzer, failOnUnsupportedField);
        if (moreLikeFields.isEmpty()) {
            return null;
        }
        mltQuery.setMoreLikeFields(moreLikeFields.toArray(Strings.EMPTY_ARRAY));

        // support for named query
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, mltQuery);
        }

        // handle like texts
        if (!likeTexts.isEmpty()) {
            mltQuery.setLikeText(likeTexts);
        }
        if (!ignoreTexts.isEmpty()) {
            mltQuery.setIgnoreText(ignoreTexts);
        }

        // handle items
        if (!likeItems.isEmpty()) {
            // set default index, type and fields if not specified
            MultiTermVectorsRequest items = likeItems;
            for (TermVectorsRequest item : ignoreItems) {
                items.add(item);
            }

            for (TermVectorsRequest item : items) {
                if (item.index() == null) {
                    item.index(parseContext.index().name());
                }
                if (item.type() == null) {
                    if (parseContext.queryTypes().size() > 1) {
                        throw new QueryParsingException(parseContext.index(),
                                "ambiguous type for item with id: " + item.id() + " and index: " + item.index());
                    } else {
                        item.type(parseContext.queryTypes().iterator().next());
                    }
                }
                // default fields if not present but don't override for artificial docs
                if (item.selectedFields() == null && item.doc() == null) {
                    if (useDefaultField) {
                        item.selectedFields("*");
                    } else {
                        item.selectedFields(moreLikeFields.toArray(new String[moreLikeFields.size()]));
                    }
                }
            }
            // fetching the items with multi-termvectors API
            BooleanQuery boolQuery = new BooleanQuery();
            MultiTermVectorsResponse responses = fetchService.fetchResponse(items);

            // getting the Fields for liked items
            mltQuery.setLikeText(MoreLikeThisFetchService.getFields(responses, likeItems));

            // getting the Fields for ignored items
            if (!ignoreItems.isEmpty()) {
                org.apache.lucene.index.Fields[] ignoreFields = MoreLikeThisFetchService.getFields(responses, ignoreItems);
                if (ignoreFields.length > 0) {
                    mltQuery.setIgnoreText(ignoreFields);
                }
            }

            boolQuery.add(mltQuery, BooleanClause.Occur.SHOULD);

            // exclude the items from the search
            if (!include) {
                handleExclude(boolQuery, likeItems);
            }
            return boolQuery;
        }

        return mltQuery;
    }

    private TermVectorsRequest parseDocument(XContentParser parser) throws IOException {
        TermVectorsRequest termVectorsRequest = newTermVectorsRequest();
        TermVectorsRequest.parseRequest(termVectorsRequest, parser);
        return termVectorsRequest;
    }

    private void parseLikeField(XContentParser parser, List<String> likeTexts, MultiTermVectorsRequest items) throws IOException {
        if (parser.currentToken().isValue()) {
            likeTexts.add(parser.text());
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            items.add(parseDocument(parser));
        } else {
            throw new ElasticsearchIllegalArgumentException("Content of 'like' parameter should either be a string or an object");
        }
    }

    private TermVectorsRequest newTermVectorsRequest() {
        return new TermVectorsRequest()
                .positions(false)
                .offsets(false)
                .payloads(false)
                .fieldStatistics(false)
                .termStatistics(false);
    }

    private List<String> removeUnsupportedFields(List<String> moreLikeFields, Analyzer analyzer, boolean failOnUnsupportedField) throws IOException {
        for (Iterator<String> it = moreLikeFields.iterator(); it.hasNext(); ) {
            final String fieldName = it.next();
            if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
                if (failOnUnsupportedField) {
                    throw new ElasticsearchIllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
                } else {
                    it.remove();
                }
            }
        }
        return moreLikeFields;
    }

    private void handleExclude(BooleanQuery boolQuery, MultiTermVectorsRequest likeItems) {
        // artificial docs get assigned a random id and should be disregarded
        List<BytesRef> uids = new ArrayList<>();
        for (TermVectorsRequest item : likeItems) {
            if (item.doc() != null) {
                continue;
            }
            uids.add(createUidAsBytes(item.type(), item.id()));
        }
        if (!uids.isEmpty()) {
            TermsFilter filter = new TermsFilter(UidFieldMapper.NAME, uids.toArray(new BytesRef[0]));
            ConstantScoreQuery query = new ConstantScoreQuery(filter);
            boolQuery.add(query, BooleanClause.Occur.MUST_NOT);
        }
    }
}
