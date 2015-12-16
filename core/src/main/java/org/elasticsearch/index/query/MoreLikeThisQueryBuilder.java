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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.termvectors.MultiTermVectorsItemResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.lucene.search.XMoreLikeThis;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.Uid.createUidAsBytes;

/**
 * A more like this query that finds documents that are "like" the provided set of document(s).
 *
 * The documents are provided as a set of strings and/or a list of {@link Item}.
 */
public class MoreLikeThisQueryBuilder extends AbstractQueryBuilder<MoreLikeThisQueryBuilder> {

    public static final String NAME = "mlt";

    public static final int DEFAULT_MAX_QUERY_TERMS = XMoreLikeThis.DEFAULT_MAX_QUERY_TERMS;
    public static final int DEFAULT_MIN_TERM_FREQ = XMoreLikeThis.DEFAULT_MIN_TERM_FREQ;
    public static final int DEFAULT_MIN_DOC_FREQ = XMoreLikeThis.DEFAULT_MIN_DOC_FREQ;
    public static final int DEFAULT_MAX_DOC_FREQ = XMoreLikeThis.DEFAULT_MAX_DOC_FREQ;
    public static final int DEFAULT_MIN_WORD_LENGTH = XMoreLikeThis.DEFAULT_MIN_WORD_LENGTH;
    public static final int DEFAULT_MAX_WORD_LENGTH = XMoreLikeThis.DEFAULT_MAX_WORD_LENGTH;
    public static final String DEFAULT_MINIMUM_SHOULD_MATCH = MoreLikeThisQuery.DEFAULT_MINIMUM_SHOULD_MATCH;
    public static final float DEFAULT_BOOST_TERMS = 0;  // no boost terms
    public static final boolean DEFAULT_INCLUDE = false;
    public static final boolean DEFAULT_FAIL_ON_UNSUPPORTED_FIELDS = true;

    // document inputs
    private final String[] fields;
    private final String[] likeTexts;
    private String[] unlikeTexts = Strings.EMPTY_ARRAY;
    private final Item[] likeItems;
    private Item[] unlikeItems = new Item[0];

    // term selection parameters
    private int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;
    private int minTermFreq = DEFAULT_MIN_TERM_FREQ;
    private int minDocFreq = DEFAULT_MIN_DOC_FREQ;
    private int maxDocFreq = DEFAULT_MAX_DOC_FREQ;
    private int minWordLength = DEFAULT_MIN_WORD_LENGTH;
    private int maxWordLength = DEFAULT_MAX_WORD_LENGTH;
    private String[] stopWords;
    private String analyzer;

    // query formation parameters
    private String minimumShouldMatch = DEFAULT_MINIMUM_SHOULD_MATCH;
    private float boostTerms = DEFAULT_BOOST_TERMS;
    private boolean include = DEFAULT_INCLUDE;

    // other parameters
    private boolean failOnUnsupportedField = DEFAULT_FAIL_ON_UNSUPPORTED_FIELDS;

    static final MoreLikeThisQueryBuilder PROTOTYPE = new MoreLikeThisQueryBuilder(new String[]{"_na_"}, null);

    /**
     * A single item to be used for a {@link MoreLikeThisQueryBuilder}.
     */
    public static final class Item implements ToXContent, Writeable<Item> {
        public static final Item[] EMPTY_ARRAY = new Item[0];

        public interface Field {
            ParseField INDEX = new ParseField("_index");
            ParseField TYPE = new ParseField("_type");
            ParseField ID = new ParseField("_id");
            ParseField DOC = new ParseField("doc");
            ParseField FIELDS = new ParseField("fields");
            ParseField PER_FIELD_ANALYZER = new ParseField("per_field_analyzer");
            ParseField ROUTING = new ParseField("_routing");
            ParseField VERSION = new ParseField("_version");
            ParseField VERSION_TYPE = new ParseField("_version_type");
        }

        private String index;
        private String type;
        private String id;
        private BytesReference doc;
        private String[] fields;
        private Map<String, String> perFieldAnalyzer;
        private String routing;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        static final Item PROTOTYPE = new Item();

        public Item() {

        }

        Item(Item copy) {
            if (copy.id == null && copy.doc == null) {
                throw new IllegalArgumentException("Item requires either id or doc to be non-null");
            }
            this.index = copy.index;
            this.type = copy.type;
            this.id = copy.id;
            this.doc = copy.doc;
            this.fields = copy.fields;
            this.perFieldAnalyzer = copy.perFieldAnalyzer;
            this.version = copy.version;
            this.versionType = copy.versionType;
        }

        /**
         * Constructor for a given item / document request
         *
         * @param index the index where the document is located
         * @param type the type of the document
         * @param id and its id
         */
        public Item(@Nullable String index, @Nullable String type, String id) {
            if (id == null) {
                throw new IllegalArgumentException("Item requires id to be non-null");
            }
            this.index = index;
            this.type = type;
            this.id = id;
        }

        /**
         * Constructor for an artificial document request, that is not present in the index.
         *
         * @param index the index to be used for parsing the doc
         * @param type the type to be used for parsing the doc
         * @param doc the document specification
         */
        public Item(@Nullable String index, @Nullable String type, XContentBuilder doc) {
            if (doc == null) {
                throw new IllegalArgumentException("Item requires doc to be non-null");
            }
            this.index = index;
            this.type = type;
            this.doc = doc.bytes();
        }

        public String index() {
            return index;
        }

        public Item index(String index) {
            this.index = index;
            return this;
        }

        public String type() {
            return type;
        }

        public Item type(String type) {
            this.type = type;
            return this;
        }

        public String id() {
            return id;
        }

        public BytesReference doc() {
            return doc;
        }

        public String[] fields() {
            return fields;
        }

        public Item fields(String... fields) {
            this.fields = fields;
            return this;
        }

        public Map<String, String> perFieldAnalyzer() {
            return perFieldAnalyzer;
        }

        /**
         * Sets the analyzer(s) to use at any given field.
         */
        public Item perFieldAnalyzer(Map<String, String> perFieldAnalyzer) {
            this.perFieldAnalyzer = perFieldAnalyzer;
            return this;
        }

        public String routing() {
            return routing;
        }

        public Item routing(String routing) {
            this.routing = routing;
            return this;
        }

        public long version() {
            return version;
        }

        public Item version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Item versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        /**
         * Convert this to a {@link TermVectorsRequest} for fetching the terms of the document.
         */
        public TermVectorsRequest toTermVectorsRequest() {
            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(index, type, id)
                    .selectedFields(fields)
                    .routing(routing)
                    .version(version)
                    .versionType(versionType)
                    .perFieldAnalyzer(perFieldAnalyzer)
                    .positions(false)  // ensures these following parameters are never set
                    .offsets(false)
                    .payloads(false)
                    .fieldStatistics(false)
                    .termStatistics(false)
                    .dfs(false);
            // for artificial docs to make sure that the id has changed in the item too
            if (doc != null) {
                termVectorsRequest.doc(doc, true);
                this.id = termVectorsRequest.id();
            }
            return termVectorsRequest;
        }

        /**
         * Parses and returns the given item.
         */
        public static Item parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher, Item item) throws IOException {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentFieldName != null) {
                    if (parseFieldMatcher.match(currentFieldName, Field.INDEX)) {
                        item.index = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, Field.TYPE)) {
                        item.type = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, Field.ID)) {
                        item.id = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, Field.DOC)) {
                        item.doc = jsonBuilder().copyCurrentStructure(parser).bytes();
                    } else if (parseFieldMatcher.match(currentFieldName, Field.FIELDS)) {
                        if (token == XContentParser.Token.START_ARRAY) {
                            List<String> fields = new ArrayList<>();
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                fields.add(parser.text());
                            }
                            item.fields(fields.toArray(new String[fields.size()]));
                        } else {
                            throw new ElasticsearchParseException(
                                    "failed to parse More Like This item. field [fields] must be an array");
                        }
                    } else if (parseFieldMatcher.match(currentFieldName, Field.PER_FIELD_ANALYZER)) {
                        item.perFieldAnalyzer(TermVectorsRequest.readPerFieldAnalyzer(parser.map()));
                    } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                        item.routing = parser.text();
                    } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                        item.version = parser.longValue();
                    } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName)
                            || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                        item.versionType = VersionType.fromString(parser.text());
                    } else {
                        throw new ElasticsearchParseException(
                                "failed to parse More Like This item. unknown field [{}]", currentFieldName);
                    }
                }
            }
            if (item.id != null && item.doc != null) {
                throw new ElasticsearchParseException(
                        "failed to parse More Like This item. either [id] or [doc] can be specified, but not both!");
            }
            if (item.id == null && item.doc == null) {
                throw new ElasticsearchParseException(
                        "failed to parse More Like This item. neither [id] nor [doc] is specified!");
            }
            return item;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (this.index != null) {
                builder.field(Field.INDEX.getPreferredName(), this.index);
            }
            if (this.type != null) {
                builder.field(Field.TYPE.getPreferredName(), this.type);
            }
            if (this.id != null) {
                builder.field(Field.ID.getPreferredName(), this.id);
            }
            if (this.doc != null) {
                XContentType contentType = XContentFactory.xContentType(this.doc);
                if (contentType == builder.contentType()) {
                    builder.rawField(Field.DOC.getPreferredName(), this.doc);
                } else {
                    XContentParser parser = XContentFactory.xContent(contentType).createParser(this.doc);
                    parser.nextToken();
                    builder.field(Field.DOC.getPreferredName());
                    builder.copyCurrentStructure(parser);
                }
            }
            if (this.fields != null) {
                builder.array(Field.FIELDS.getPreferredName(), this.fields);
            }
            if (this.perFieldAnalyzer != null) {
                builder.field(Field.PER_FIELD_ANALYZER.getPreferredName(), this.perFieldAnalyzer);
            }
            if (this.routing != null) {
                builder.field(Field.ROUTING.getPreferredName(), this.routing);
            }
            if (this.version != Versions.MATCH_ANY) {
                builder.field(Field.VERSION.getPreferredName(), this.version);
            }
            if (this.versionType != VersionType.INTERNAL) {
                builder.field(Field.VERSION_TYPE.getPreferredName(), this.versionType.toString().toLowerCase(Locale.ROOT));
            }
            return builder.endObject();
        }

        @Override
        public final String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                toXContent(builder, EMPTY_PARAMS);
                return builder.string();
            } catch (Exception e) {
                return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }

        @Override
        public Item readFrom(StreamInput in) throws IOException {
            Item item = new Item();
            item.index = in.readOptionalString();
            item.type = in.readOptionalString();
            if (in.readBoolean()) {
                item.doc = (BytesReference) in.readGenericValue();
            } else {
                item.id = in.readString();
            }
            item.fields = in.readOptionalStringArray();
            item.perFieldAnalyzer = (Map<String, String>) in.readGenericValue();
            item.routing = in.readOptionalString();
            item.version = in.readLong();
            item.versionType = VersionType.readVersionTypeFrom(in);
            return item;
        }

        public static Item readItemFrom(StreamInput in) throws IOException {
            return PROTOTYPE.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(index);
            out.writeOptionalString(type);
            out.writeBoolean(doc != null);
            if (doc != null) {
                out.writeGenericValue(doc);
            } else {
                out.writeString(id);
            }
            out.writeOptionalStringArray(fields);
            out.writeGenericValue(perFieldAnalyzer);
            out.writeOptionalString(routing);
            out.writeLong(version);
            versionType.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, type, id, doc, Arrays.hashCode(fields), perFieldAnalyzer, routing,
                    version, versionType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Item)) return false;
            Item other = (Item) o;
            return Objects.equals(index, other.index) &&
                    Objects.equals(type, other.type) &&
                    Objects.equals(id, other.id) &&
                    Objects.equals(doc, other.doc) &&
                    Arrays.equals(fields, other.fields) &&  // otherwise we are comparing pointers
                    Objects.equals(perFieldAnalyzer, other.perFieldAnalyzer) &&
                    Objects.equals(routing, other.routing) &&
                    Objects.equals(version, other.version) &&
                    Objects.equals(versionType, other.versionType);
        }
    }

    /**
     * Constructs a new more like this query which uses the "_all" field.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder(String[] likeTexts, Item[] likeItems) {
        this(null, likeTexts, likeItems);
    }

    /**
     * Sets the field names that will be used when generating the 'More Like This' query.
     *
     * @param fields the field names that will be used when generating the 'More Like This' query.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder(@Nullable String[] fields, @Nullable String[] likeTexts, @Nullable Item[] likeItems) {
        // TODO we allow null here for the _all field, but this is forbidden in the parser. Re-check
        if (fields != null && fields.length == 0) {
            throw new IllegalArgumentException("mlt query requires 'fields' to be specified");
        }
        if ((likeTexts == null || likeTexts.length == 0) && (likeItems == null || likeItems.length == 0)) {
            throw new IllegalArgumentException("mlt query requires either 'like' texts or items to be specified.");
        }
        this.fields = fields;
        this.likeTexts = Optional.ofNullable(likeTexts).orElse(Strings.EMPTY_ARRAY);
        this.likeItems = Optional.ofNullable(likeItems).orElse(new Item[0]);
    }

    public String[] fields() {
        return this.fields;
    }

    public String[] likeTexts() {
        return likeTexts;
    }

    public Item[] likeItems() {
        return likeItems;
    }

    /**
     * Sets the text from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder unlike(String[] unlikeTexts) {
        this.unlikeTexts = Optional.ofNullable(unlikeTexts).orElse(Strings.EMPTY_ARRAY);
        return this;
    }

    public String[] unlikeTexts() {
        return unlikeTexts;
    }

    /**
     * Sets the documents from which the terms should not be selected from.
     */
    public MoreLikeThisQueryBuilder unlike(Item[] unlikeItems) {
        this.unlikeItems = Optional.ofNullable(unlikeItems).orElse(new Item[0]);
        return this;
    }

    public Item[] unlikeItems() {
        return unlikeItems;
    }

    /**
     * Sets the maximum number of query terms that will be included in any generated query.
     * Defaults to <tt>25</tt>.
     */
    public MoreLikeThisQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    public int maxQueryTerms() {
        return maxQueryTerms;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. The default
     * frequency is <tt>2</tt>.
     */
    public MoreLikeThisQueryBuilder minTermFreq(int minTermFreq) {
        this.minTermFreq = minTermFreq;
        return this;
    }

    public int minTermFreq() {
        return minTermFreq;
    }

    /**
     * Sets the frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisQueryBuilder minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    public int minDocFreq() {
        return minDocFreq;
    }

    /**
     * Set the maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisQueryBuilder maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    public int maxDocFreq() {
        return maxDocFreq;
    }

    /**
     * Sets the minimum word length below which words will be ignored. Defaults
     * to <tt>0</tt>.
     */
    public MoreLikeThisQueryBuilder minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    public int minWordLength() {
        return minWordLength;
    }

    /**
     * Sets the maximum word length above which words will be ignored. Defaults to
     * unbounded (<tt>0</tt>).
     */
    public MoreLikeThisQueryBuilder maxWordLength(int maxWordLength) {
        this.maxWordLength = maxWordLength;
        return this;
    }

    public int maxWordLength() {
        return maxWordLength;
    }

    /**
     * Set the set of stopwords.
     * <p>
     * Any word in this set is considered "uninteresting" and ignored. Even if your Analyzer allows stopwords, you
     * might want to tell the MoreLikeThis code to ignore them, as for the purposes of document similarity it seems
     * reasonable to assume that "a stop word is never interesting".
     */
    public MoreLikeThisQueryBuilder stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    public MoreLikeThisQueryBuilder stopWords(List<String> stopWords) {
        if (stopWords == null) {
            throw new IllegalArgumentException("requires stopwords to be non-null");
        }
        this.stopWords = stopWords.toArray(new String[stopWords.size()]);
        return this;
    }

    public String[] stopWords() {
        return stopWords;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the fied.
     */
    public MoreLikeThisQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return analyzer;
    }

    /**
     * Number of terms that must match the generated query expressed in the
     * common syntax for minimum should match. Defaults to <tt>30%</tt>.
     *
     * @see    org.elasticsearch.common.lucene.search.Queries#calculateMinShouldMatch(int, String)
     */
    public MoreLikeThisQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        if (minimumShouldMatch == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires minimum should match to be non-null");
        }
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public String minimumShouldMatch() {
        return minimumShouldMatch;
    }

    /**
     * Sets the boost factor to use when boosting terms. Defaults to <tt>0</tt> (deactivated).
     */
    public MoreLikeThisQueryBuilder boostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    public float boostTerms() {
        return boostTerms;
    }

    /**
     * Whether to include the input documents. Defaults to <tt>false</tt>
     */
    public MoreLikeThisQueryBuilder include(boolean include) {
        this.include = include;
        return this;
    }

    public boolean include() {
        return include;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     */
    public MoreLikeThisQueryBuilder failOnUnsupportedField(boolean fail) {
        this.failOnUnsupportedField = fail;
        return this;
    }

    public boolean failOnUnsupportedField() {
        return failOnUnsupportedField;
    }

    /**
     * Converts an array of String ids to and Item[].
     * @param ids the ids to convert
     * @return the new items array
     * @deprecated construct the items array externaly and use it in the constructor / setter
     */
    @Deprecated
    public static Item[] ids(String... ids) {
        Item[] items = new Item[ids.length];
        for (int i = 0; i < items.length; i++) {
            items[i] = new Item(null, null, ids[i]);
        }
        return items;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (fields != null) {
            builder.field(MoreLikeThisQueryParser.Field.FIELDS.getPreferredName(), fields);
        }
        buildLikeField(builder, MoreLikeThisQueryParser.Field.LIKE.getPreferredName(), likeTexts, likeItems);
        buildLikeField(builder, MoreLikeThisQueryParser.Field.UNLIKE.getPreferredName(), unlikeTexts, unlikeItems);
        builder.field(MoreLikeThisQueryParser.Field.MAX_QUERY_TERMS.getPreferredName(), maxQueryTerms);
        builder.field(MoreLikeThisQueryParser.Field.MIN_TERM_FREQ.getPreferredName(), minTermFreq);
        builder.field(MoreLikeThisQueryParser.Field.MIN_DOC_FREQ.getPreferredName(), minDocFreq);
        builder.field(MoreLikeThisQueryParser.Field.MAX_DOC_FREQ.getPreferredName(), maxDocFreq);
        builder.field(MoreLikeThisQueryParser.Field.MIN_WORD_LENGTH.getPreferredName(), minWordLength);
        builder.field(MoreLikeThisQueryParser.Field.MAX_WORD_LENGTH.getPreferredName(), maxWordLength);
        if (stopWords != null) {
            builder.field(MoreLikeThisQueryParser.Field.STOP_WORDS.getPreferredName(), stopWords);
        }
        if (analyzer != null) {
            builder.field(MoreLikeThisQueryParser.Field.ANALYZER.getPreferredName(), analyzer);
        }
        builder.field(MoreLikeThisQueryParser.Field.MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        builder.field(MoreLikeThisQueryParser.Field.BOOST_TERMS.getPreferredName(), boostTerms);
        builder.field(MoreLikeThisQueryParser.Field.INCLUDE.getPreferredName(), include);
        builder.field(MoreLikeThisQueryParser.Field.FAIL_ON_UNSUPPORTED_FIELD.getPreferredName(), failOnUnsupportedField);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static void buildLikeField(XContentBuilder builder, String fieldName, String[] texts, Item[] items) throws IOException {
        if (texts.length > 0 || items.length > 0) {
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

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Item[] likeItems = new Item[this.likeItems.length];
        for (int i = 0; i < likeItems.length; i++) {
            likeItems[i] = new Item(this.likeItems[i]);
        }
        Item[] unlikeItems = new Item[this.unlikeItems.length];
        for (int i = 0; i < unlikeItems.length; i++) {
            unlikeItems[i] = new Item(this.unlikeItems[i]);
        }

        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();

        // set similarity
        mltQuery.setSimilarity(context.getSearchSimilarity());

        // set query parameters
        mltQuery.setMaxQueryTerms(maxQueryTerms);
        mltQuery.setMinTermFrequency(minTermFreq);
        mltQuery.setMinDocFreq(minDocFreq);
        mltQuery.setMaxDocFreq(maxDocFreq);
        mltQuery.setMinWordLen(minWordLength);
        mltQuery.setMaxWordLen(maxWordLength);
        mltQuery.setMinimumShouldMatch(minimumShouldMatch);
        if (stopWords != null) {
            mltQuery.setStopWords(new HashSet<>(Arrays.asList(stopWords)));
        }

        // sets boost terms
        if (boostTerms != 0) {
            mltQuery.setBoostTerms(true);
            mltQuery.setBoostTermsFactor(boostTerms);
        }

        // set analyzer
        Analyzer analyzerObj = context.getAnalysisService().analyzer(analyzer);
        if (analyzerObj == null) {
            analyzerObj = context.getMapperService().searchAnalyzer();
        }
        mltQuery.setAnalyzer(analyzerObj);

        // set like text fields
        boolean useDefaultField = (fields == null);
        List<String> moreLikeFields = new ArrayList<>();
        if (useDefaultField) {
            moreLikeFields = Collections.singletonList(context.defaultField());
        } else {
            for (String field : fields) {
                MappedFieldType fieldType = context.fieldMapper(field);
                moreLikeFields.add(fieldType == null ? field : fieldType.name());
            }
        }

        // possibly remove unsupported fields
        removeUnsupportedFields(moreLikeFields, analyzerObj, failOnUnsupportedField);
        if (moreLikeFields.isEmpty()) {
            return null;
        }
        mltQuery.setMoreLikeFields(moreLikeFields.toArray(Strings.EMPTY_ARRAY));

        // handle like texts
        if (likeTexts.length > 0) {
            mltQuery.setLikeText(likeTexts);
        }
        if (unlikeTexts.length > 0) {
            mltQuery.setUnlikeText(unlikeTexts);
        }

        // handle items
        if (likeItems.length > 0) {
            return handleItems(context, mltQuery, likeItems, unlikeItems, include, moreLikeFields, useDefaultField);
        } else {
            return mltQuery;
        }
    }

    private static List<String> removeUnsupportedFields(List<String> moreLikeFields, Analyzer analyzer, boolean failOnUnsupportedField) throws IOException {
        for (Iterator<String> it = moreLikeFields.iterator(); it.hasNext(); ) {
            final String fieldName = it.next();
            if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
                if (failOnUnsupportedField) {
                    throw new IllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
                } else {
                    it.remove();
                }
            }
        }
        return moreLikeFields;
    }

    private Query handleItems(QueryShardContext context, MoreLikeThisQuery mltQuery, Item[] likeItems, Item[] unlikeItems,
                              boolean include, List<String> moreLikeFields, boolean useDefaultField) throws IOException {
        // set default index, type and fields if not specified
        for (Item item : likeItems) {
            setDefaultIndexTypeFields(context, item, moreLikeFields, useDefaultField);
        }
        for (Item item : unlikeItems) {
            setDefaultIndexTypeFields(context, item, moreLikeFields, useDefaultField);
        }

        // fetching the items with multi-termvectors API
        MultiTermVectorsResponse responses = fetchResponse(context.getClient(), likeItems, unlikeItems, SearchContext.current());

        // getting the Fields for liked items
        mltQuery.setLikeText(getFieldsFor(responses, likeItems));

        // getting the Fields for unliked items
        if (unlikeItems.length > 0) {
            org.apache.lucene.index.Fields[] unlikeFields = getFieldsFor(responses, unlikeItems);
            if (unlikeFields.length > 0) {
                mltQuery.setUnlikeText(unlikeFields);
            }
        }

        BooleanQuery boolQuery = new BooleanQuery();
        boolQuery.add(mltQuery, BooleanClause.Occur.SHOULD);

        // exclude the items from the search
        if (!include) {
            handleExclude(boolQuery, likeItems);
        }
        return boolQuery;
    }

    private static void setDefaultIndexTypeFields(QueryShardContext context, Item item, List<String> moreLikeFields,
                                                  boolean useDefaultField) {
        if (item.index() == null) {
            item.index(context.index().name());
        }
        if (item.type() == null) {
            if (context.queryTypes().size() > 1) {
                throw new QueryShardException(context,
                        "ambiguous type for item with id: " + item.id() + " and index: " + item.index());
            } else {
                item.type(context.queryTypes().iterator().next());
            }
        }
        // default fields if not present but don't override for artificial docs
        if ((item.fields() == null || item.fields().length == 0) && item.doc() == null) {
            if (useDefaultField) {
                item.fields("*");
            } else {
                item.fields(moreLikeFields.toArray(new String[moreLikeFields.size()]));
            }
        }
    }

    private MultiTermVectorsResponse fetchResponse(Client client, Item[] likeItems, @Nullable Item[] unlikeItems,
                                                   SearchContext searchContext) throws IOException {
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        for (Item item : likeItems) {
            request.add(item.toTermVectorsRequest());
        }
        for (Item item : unlikeItems) {
            request.add(item.toTermVectorsRequest());
        }
        request.copyContextAndHeadersFrom(searchContext);
        return client.multiTermVectors(request).actionGet();
    }

    private static Fields[] getFieldsFor(MultiTermVectorsResponse responses, Item[] items) throws IOException {
        List<Fields> likeFields = new ArrayList<>();

        Set<Item> selectedItems = new HashSet<>();
        for (Item request : items) {
            selectedItems.add(new Item(request.index(), request.type(), request.id()));
        }

        for (MultiTermVectorsItemResponse response : responses) {
            if (!hasResponseFromRequest(response, selectedItems)) {
                continue;
            }
            if (response.isFailed()) {
                continue;
            }
            TermVectorsResponse getResponse = response.getResponse();
            if (!getResponse.isExists()) {
                continue;
            }
            likeFields.add(getResponse.getFields());
        }
        return likeFields.toArray(Fields.EMPTY_ARRAY);
    }

    private static boolean hasResponseFromRequest(MultiTermVectorsItemResponse response, Set<Item> selectedItems) {
        return selectedItems.contains(new Item(response.getIndex(), response.getType(), response.getId()));
    }

    private static void handleExclude(BooleanQuery boolQuery, Item[] likeItems) {
        // artificial docs get assigned a random id and should be disregarded
        List<BytesRef> uids = new ArrayList<>();
        for (Item item : likeItems) {
            if (item.doc() != null) {
                continue;
            }
            uids.add(createUidAsBytes(item.type(), item.id()));
        }
        if (!uids.isEmpty()) {
            TermsQuery query = new TermsQuery(UidFieldMapper.NAME, uids.toArray(new BytesRef[0]));
            boolQuery.add(query, BooleanClause.Occur.MUST_NOT);
        }
    }

    @Override
    protected MoreLikeThisQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String[] fields = in.readOptionalStringArray();
        String[] likeTexts = in.readStringArray();
        Item[] likeItems = readItems(in);
        MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = new MoreLikeThisQueryBuilder(fields, likeTexts, likeItems);
        moreLikeThisQueryBuilder.unlikeTexts = in.readStringArray();
        moreLikeThisQueryBuilder.unlikeItems = readItems(in);
        moreLikeThisQueryBuilder.maxQueryTerms = in.readVInt();
        moreLikeThisQueryBuilder.minTermFreq = in.readVInt();
        moreLikeThisQueryBuilder.minDocFreq = in.readVInt();
        moreLikeThisQueryBuilder.maxDocFreq = in.readVInt();
        moreLikeThisQueryBuilder.minWordLength = in.readVInt();
        moreLikeThisQueryBuilder.maxWordLength = in.readVInt();
        moreLikeThisQueryBuilder.stopWords = in.readOptionalStringArray();
        moreLikeThisQueryBuilder.analyzer = in.readOptionalString();
        moreLikeThisQueryBuilder.minimumShouldMatch = in.readString();
        moreLikeThisQueryBuilder.boostTerms = (Float) in.readGenericValue();
        moreLikeThisQueryBuilder.include = in.readBoolean();
        moreLikeThisQueryBuilder.failOnUnsupportedField = in.readBoolean();
        return moreLikeThisQueryBuilder;
    }

    private static Item[] readItems(StreamInput in) throws IOException {
        int size = in.readVInt();
        Item[] items = new Item[size];
        for (int i = 0; i < size; i++) {
            items[i] = Item.readItemFrom(in);
        }
        return items;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalStringArray(fields);
        out.writeStringArray(likeTexts);
        writeItems(likeItems, out);
        out.writeStringArray(unlikeTexts);
        writeItems(unlikeItems, out);
        out.writeVInt(maxQueryTerms);
        out.writeVInt(minTermFreq);
        out.writeVInt(minDocFreq);
        out.writeVInt(maxDocFreq);
        out.writeVInt(minWordLength);
        out.writeVInt(maxWordLength);
        out.writeOptionalStringArray(stopWords);
        out.writeOptionalString(analyzer);
        out.writeString(minimumShouldMatch);
        out.writeGenericValue(boostTerms);
        out.writeBoolean(include);
        out.writeBoolean(failOnUnsupportedField);
    }

    private static void writeItems(Item[] items, StreamOutput out) throws IOException {
        out.writeVInt(items.length);
        for (Item item : items) {
            item.writeTo(out);
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(Arrays.hashCode(fields), Arrays.hashCode(likeTexts),
                Arrays.hashCode(unlikeTexts), Arrays.hashCode(likeItems), Arrays.hashCode(unlikeItems),
                maxQueryTerms, minTermFreq, minDocFreq, maxDocFreq, minWordLength, maxWordLength,
                Arrays.hashCode(stopWords), analyzer, minimumShouldMatch, boostTerms, include, failOnUnsupportedField);
    }

    @Override
    protected boolean doEquals(MoreLikeThisQueryBuilder other) {
        return Arrays.equals(fields, other.fields) &&
                Arrays.equals(likeTexts, other.likeTexts) &&
                Arrays.equals(unlikeTexts, other.unlikeTexts) &&
                Arrays.equals(likeItems, other.likeItems) &&
                Arrays.equals(unlikeItems, other.unlikeItems) &&
                Objects.equals(maxQueryTerms, other.maxQueryTerms) &&
                Objects.equals(minTermFreq, other.minTermFreq) &&
                Objects.equals(minDocFreq, other.minDocFreq) &&
                Objects.equals(maxDocFreq, other.maxDocFreq) &&
                Objects.equals(minWordLength, other.minWordLength) &&
                Objects.equals(maxWordLength, other.maxWordLength) &&
                Arrays.equals(stopWords, other.stopWords) &&  // otherwise we are comparing pointers
                Objects.equals(analyzer, other.analyzer) &&
                Objects.equals(minimumShouldMatch, other.minimumShouldMatch) &&
                Objects.equals(boostTerms, other.boostTerms) &&
                Objects.equals(include, other.include) &&
                Objects.equals(failOnUnsupportedField, other.failOnUnsupportedField);
    }
}
