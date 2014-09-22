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

package org.elasticsearch.action.mlt;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * A more like this request allowing to search for documents that a "like" the provided document. The document
 * to check against to fetched based on the index, type and id provided. Best created with {@link org.elasticsearch.client.Requests#moreLikeThisRequest(String)}.
 * <p/>
 * <p>Note, the {@link #index()}, {@link #type(String)} and {@link #id(String)} are required.
 *
 * @see org.elasticsearch.client.Client#moreLikeThis(MoreLikeThisRequest)
 * @see org.elasticsearch.client.Requests#moreLikeThisRequest(String)
 * @see org.elasticsearch.action.search.SearchResponse
 */
public class MoreLikeThisRequest extends ActionRequest<MoreLikeThisRequest> implements CompositeIndicesRequest {

    private String index;

    private String type;

    private String id;

    private String routing;

    private String[] fields;

    private float percentTermsToMatch = -1;
    private int minTermFreq = -1;
    private int maxQueryTerms = -1;
    private String[] stopWords = null;
    private int minDocFreq = -1;
    private int maxDocFreq = -1;
    private int minWordLength = -1;
    private int maxWordLength = -1;
    private float boostTerms = -1;
    private boolean include = false;

    private SearchType searchType = SearchType.DEFAULT;
    private int searchSize = 0;
    private int searchFrom = 0;
    private String[] searchIndices;
    private String[] searchTypes;
    private Scroll searchScroll;

    private BytesReference searchSource;
    private boolean searchSourceUnsafe;

    MoreLikeThisRequest() {
    }

    /**
     * Constructs a new more like this request for a document that will be fetch from the provided index.
     * Use {@link #type(String)} and {@link #id(String)} to specify the document to load.
     */
    public MoreLikeThisRequest(String index) {
        this.index = index;
    }

    /**
     * The index to load the document from which the "like" query will run with.
     */
    public String index() {
        return index;
    }

    /**
     * The type of document to load from which the "like" query will run with.
     */
    public String type() {
        return type;
    }

    void index(String index) {
        this.index = index;
    }

    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        //we create two fake indices subrequests as we don't have the actual ones yet
        //since they get created later on in TransportMoreLikeThisAction
        List<IndicesRequest> requests = Lists.newArrayList();
        requests.add(new IndicesRequest() {
            @Override
            public String[] indices() {
                return new String[]{index};
            }

            @Override
            public IndicesOptions indicesOptions() {
                return MoreLikeThisRequest.this.indicesOptions();
            }
        });
        requests.add(new IndicesRequest.Replaceable() {
            @Override
            public String[] indices() {
                if (searchIndices != null) {
                    return searchIndices;
                }
                return new String[]{index};
            }

            @Override
            public IndicesRequest indices(String[] indices) {
                searchIndices = indices;
                return this;
            }

            @Override
            public IndicesOptions indicesOptions() {
                return SearchRequest.DEFAULT_INDICES_OPTIONS;
            }
        });
        return requests;
    }

    /**
     * The type of document to load from which the "like" query will execute with.
     */
    public MoreLikeThisRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of document to load from which the "like" query will execute with.
     */
    public String id() {
        return id;
    }

    /**
     * The id of document to load from which the "like" query will execute with.
     */
    public MoreLikeThisRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * @return The routing for this request. This used for the `get` part of the mlt request.
     */
    public String routing() {
        return routing;
    }

    public void routing(String routing) {
        this.routing = routing;
    }

    /**
     * The fields of the document to use in order to find documents "like" this one. Defaults to run
     * against all the document fields.
     */
    public String[] fields() {
        return this.fields;
    }

    /**
     * The fields of the document to use in order to find documents "like" this one. Defaults to run
     * against all the document fields.
     */
    public MoreLikeThisRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * The percent of the terms to match for each field. Defaults to <tt>0.3f</tt>.
     */
    public MoreLikeThisRequest percentTermsToMatch(float percentTermsToMatch) {
        this.percentTermsToMatch = percentTermsToMatch;
        return this;
    }

    /**
     * The percent of the terms to match for each field. Defaults to <tt>0.3f</tt>.
     */
    public float percentTermsToMatch() {
        return this.percentTermsToMatch;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. Defaults to <tt>2</tt>.
     */
    public MoreLikeThisRequest minTermFreq(int minTermFreq) {
        this.minTermFreq = minTermFreq;
        return this;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. Defaults to <tt>2</tt>.
     */
    public int minTermFreq() {
        return this.minTermFreq;
    }

    /**
     * The maximum number of query terms that will be included in any generated query. Defaults to <tt>25</tt>.
     */
    public MoreLikeThisRequest maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * The maximum number of query terms that will be included in any generated query. Defaults to <tt>25</tt>.
     */
    public int maxQueryTerms() {
        return this.maxQueryTerms;
    }

    /**
     * Any word in this set is considered "uninteresting" and ignored.
     * <p/>
     * <p>Even if your Analyzer allows stopwords, you might want to tell the MoreLikeThis code to ignore them, as
     * for the purposes of document similarity it seems reasonable to assume that "a stop word is never interesting".
     * <p/>
     * <p>Defaults to no stop words.
     */
    public MoreLikeThisRequest stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    /**
     * Any word in this set is considered "uninteresting" and ignored.
     * <p/>
     * <p>Even if your Analyzer allows stopwords, you might want to tell the MoreLikeThis code to ignore them, as
     * for the purposes of document similarity it seems reasonable to assume that "a stop word is never interesting".
     * <p/>
     * <p>Defaults to no stop words.
     */
    public String[] stopWords() {
        return this.stopWords;
    }

    /**
     * The frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisRequest minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * The frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public int minDocFreq() {
        return this.minDocFreq;
    }

    /**
     * The maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisRequest maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    /**
     * The maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public int maxDocFreq() {
        return this.maxDocFreq;
    }

    /**
     * The minimum word length below which words will be ignored. Defaults to <tt>0</tt>.
     */
    public MoreLikeThisRequest minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * The minimum word length below which words will be ignored. Defaults to <tt>0</tt>.
     */
    public int minWordLength() {
        return this.minWordLength;
    }

    /**
     * The maximum word length above which words will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisRequest maxWordLength(int maxWordLength) {
        this.maxWordLength = maxWordLength;
        return this;
    }

    /**
     * The maximum word length above which words will be ignored. Defaults to unbounded.
     */
    public int maxWordLength() {
        return this.maxWordLength;
    }

    /**
     * The boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisRequest boostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * The boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public float boostTerms() {
        return this.boostTerms;
    }

    /**
     * Whether to include the queried document. Defaults to <tt>false</tt>.
     */
    public MoreLikeThisRequest include(boolean include) {
        this.include = include;
        return this;
    }

    /**
     * Whether to include the queried document. Defaults to <tt>false</tt>.
     */
    public boolean include() {
        return this.include;
    }

    void beforeLocalFork() {
        if (searchSourceUnsafe) {
            searchSource = searchSource.copyBytesArray();
            searchSourceUnsafe = false;
        }
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest searchSource(SearchSourceBuilder sourceBuilder) {
        this.searchSource = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        this.searchSourceUnsafe = false;
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest searchSource(String searchSource) {
        this.searchSource = new BytesArray(searchSource);
        this.searchSourceUnsafe = false;
        return this;
    }

    public MoreLikeThisRequest searchSource(Map searchSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(searchSource);
            return searchSource(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + searchSource + "]", e);
        }
    }

    public MoreLikeThisRequest searchSource(XContentBuilder builder) {
        this.searchSource = builder.bytes();
        this.searchSourceUnsafe = false;
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest searchSource(byte[] searchSource) {
        return searchSource(searchSource, 0, searchSource.length, false);
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest searchSource(byte[] searchSource, int offset, int length, boolean unsafe) {
        return searchSource(new BytesArray(searchSource, offset, length), unsafe);
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest searchSource(BytesReference searchSource, boolean unsafe) {
        this.searchSource = searchSource;
        this.searchSourceUnsafe = unsafe;
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public BytesReference searchSource() {
        return this.searchSource;
    }

    public boolean searchSourceUnsafe() {
        return searchSourceUnsafe;
    }

    /**
     * The search type of the mlt search query.
     */
    public MoreLikeThisRequest searchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    /**
     * The search type of the mlt search query.
     */
    public MoreLikeThisRequest searchType(String searchType) throws ElasticsearchIllegalArgumentException {
        return searchType(SearchType.fromString(searchType));
    }

    /**
     * The search type of the mlt search query.
     */
    public SearchType searchType() {
        return this.searchType;
    }

    /**
     * The indices the resulting mlt query will run against. If not set, will run
     * against the index the document was fetched from.
     */
    public MoreLikeThisRequest searchIndices(String... searchIndices) {
        this.searchIndices = searchIndices;
        return this;
    }

    /**
     * The indices the resulting mlt query will run against. If not set, will run
     * against the index the document was fetched from.
     */
    public String[] searchIndices() {
        return this.searchIndices;
    }

    /**
     * The types the resulting mlt query will run against. If not set, will run
     * against the type of the document fetched.
     */
    public MoreLikeThisRequest searchTypes(String... searchTypes) {
        this.searchTypes = searchTypes;
        return this;
    }

    /**
     * The types the resulting mlt query will run against. If not set, will run
     * against the type of the document fetched.
     */
    public String[] searchTypes() {
        return this.searchTypes;
    }

    /**
     * An optional search scroll request to be able to continue and scroll the search
     * operation.
     */
    public MoreLikeThisRequest searchScroll(Scroll searchScroll) {
        this.searchScroll = searchScroll;
        return this;
    }

    /**
     * An optional search scroll request to be able to continue and scroll the search
     * operation.
     */
    public Scroll searchScroll() {
        return this.searchScroll;
    }

    /**
     * The number of documents to return, defaults to 10.
     */
    public MoreLikeThisRequest searchSize(int size) {
        this.searchSize = size;
        return this;
    }

    public int searchSize() {
        return this.searchSize;
    }

    /**
     * From which search result set to return.
     */
    public MoreLikeThisRequest searchFrom(int from) {
        this.searchFrom = from;
        return this;
    }

    public int searchFrom() {
        return this.searchFrom;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = ValidateActions.addValidationError("index is missing", validationException);
        }
        if (type == null) {
            validationException = ValidateActions.addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = ValidateActions.addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        type = in.readString();
        id = in.readString();
        // no need to pass threading over the network, they are always false when coming throw a thread pool
        int size = in.readVInt();
        if (size == 0) {
            fields = Strings.EMPTY_ARRAY;
        } else {
            fields = new String[size];
            for (int i = 0; i < size; i++) {
                fields[i] = in.readString();
            }
        }

        percentTermsToMatch = in.readFloat();
        minTermFreq = in.readVInt();
        maxQueryTerms = in.readVInt();
        size = in.readVInt();
        if (size > 0) {
            stopWords = new String[size];
            for (int i = 0; i < size; i++) {
                stopWords[i] = in.readString();
            }
        }
        minDocFreq = in.readVInt();
        maxDocFreq = in.readVInt();
        minWordLength = in.readVInt();
        maxWordLength = in.readVInt();
        boostTerms = in.readFloat();
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            include = in.readBoolean();
        } else {
            include = false; // hard-coded behavior until Elasticsearch 1.2
        }

        searchType = SearchType.fromId(in.readByte());
        if (in.getVersion().before(Version.V_1_4_0_Beta1)) {
            //searchQueryHint was unused and removed in 1.4
            if (in.readBoolean()) {
                in.readString();
            }
        }
        size = in.readVInt();
        if (size == 0) {
            searchIndices = null;
        } else if (size == 1) {
            searchIndices = Strings.EMPTY_ARRAY;
        } else {
            searchIndices = new String[size - 1];
            for (int i = 0; i < searchIndices.length; i++) {
                searchIndices[i] = in.readString();
            }
        }
        size = in.readVInt();
        if (size == 0) {
            searchTypes = null;
        } else if (size == 1) {
            searchTypes = Strings.EMPTY_ARRAY;
        } else {
            searchTypes = new String[size - 1];
            for (int i = 0; i < searchTypes.length; i++) {
                searchTypes[i] = in.readString();
            }
        }
        if (in.readBoolean()) {
            searchScroll = readScroll(in);
        }

        searchSourceUnsafe = false;
        searchSource = in.readBytesReference();

        searchSize = in.readVInt();
        searchFrom = in.readVInt();
        routing = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(type);
        out.writeString(id);
        if (fields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(fields.length);
            for (String field : fields) {
                out.writeString(field);
            }
        }

        out.writeFloat(percentTermsToMatch);
        out.writeVInt(minTermFreq);
        out.writeVInt(maxQueryTerms);
        if (stopWords == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(stopWords.length);
            for (String stopWord : stopWords) {
                out.writeString(stopWord);
            }
        }
        out.writeVInt(minDocFreq);
        out.writeVInt(maxDocFreq);
        out.writeVInt(minWordLength);
        out.writeVInt(maxWordLength);
        out.writeFloat(boostTerms);
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeBoolean(include);
        }

        out.writeByte(searchType.id());
        if (out.getVersion().before(Version.V_1_4_0_Beta1)) {
            //searchQueryHint was unused and removed in 1.4
            out.writeBoolean(false);
        }
        if (searchIndices == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(searchIndices.length + 1);
            for (String index : searchIndices) {
                out.writeString(index);
            }
        }
        if (searchTypes == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(searchTypes.length + 1);
            for (String type : searchTypes) {
                out.writeString(type);
            }
        }
        if (searchScroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            searchScroll.writeTo(out);
        }
        out.writeBytesReference(searchSource);

        out.writeVInt(searchSize);
        out.writeVInt(searchFrom);
        out.writeOptionalString(routing);
    }
}
