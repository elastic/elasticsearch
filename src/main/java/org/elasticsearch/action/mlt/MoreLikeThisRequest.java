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

package org.elasticsearch.action.mlt;

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * A more like this request allowing to search for documents that a "like" the provided document. The document
 * to check against to fetched based on the index, type and id provided. Best created with {@link org.elasticsearch.client.Requests#moreLikeThisRequest(String)}.
 * <p/>
 * <p>Note, the {@link #getIndex()}, {@link #setType(String)} and {@link #setId(String)} are required.
 *
 * @see org.elasticsearch.client.Client#moreLikeThis(MoreLikeThisRequest)
 * @see org.elasticsearch.client.Requests#moreLikeThisRequest(String)
 * @see org.elasticsearch.action.search.SearchResponse
 */
public class MoreLikeThisRequest extends ActionRequest<MoreLikeThisRequest> {

    private static final XContentType contentType = Requests.CONTENT_TYPE;

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
    private int minWordLen = -1;
    private int maxWordLen = -1;
    private float boostTerms = -1;

    private SearchType searchType = SearchType.DEFAULT;
    private int searchSize = 0;
    private int searchFrom = 0;
    private String searchQueryHint;
    private String[] searchIndices;
    private String[] searchTypes;
    private Scroll searchScroll;

    private BytesReference searchSource;
    private boolean searchSourceUnsafe;

    MoreLikeThisRequest() {
    }

    /**
     * Constructs a new more like this request for a document that will be fetch from the provided index.
     * Use {@link #setType(String)} and {@link #setId(String)} to specify the document to load.
     */
    public MoreLikeThisRequest(String index) {
        this.index = index;
    }

    /**
     * The index to load the document from which the "like" query will run with.
     */
    public String getIndex() {
        return index;
    }

    /**
     * The type of document to load from which the "like" query will run with.
     */
    public String getType() {
        return type;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    /**
     * The type of document to load from which the "like" query will execute with.
     */
    @Required
    public MoreLikeThisRequest setType(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of document to load from which the "like" query will execute with.
     */
    public String getId() {
        return id;
    }

    /**
     * The id of document to load from which the "like" query will execute with.
     */
    @Required
    public MoreLikeThisRequest setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * @return The routing for this request. This used for the `get` part of the mlt request.
     */
    public String getRouting() {
        return routing;
    }

    public void setRouting(String routing) {
        this.routing = routing;
    }

    /**
     * The fields of the document to use in order to find documents "like" this one. Defaults to run
     * against all the document fields.
     */
    public String[] getFields() {
        return this.fields;
    }

    /**
     * The fields of the document to use in order to find documents "like" this one. Defaults to run
     * against all the document fields.
     */
    public MoreLikeThisRequest setFields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * The percent of the terms to match for each field. Defaults to <tt>0.3f</tt>.
     */
    public MoreLikeThisRequest setPercentTermsToMatch(float percentTermsToMatch) {
        this.percentTermsToMatch = percentTermsToMatch;
        return this;
    }

    /**
     * The percent of the terms to match for each field. Defaults to <tt>0.3f</tt>.
     */
    public float getPercentTermsToMatch() {
        return this.percentTermsToMatch;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. Defaults to <tt>2</tt>.
     */
    public MoreLikeThisRequest setMinTermFreq(int minTermFreq) {
        this.minTermFreq = minTermFreq;
        return this;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. Defaults to <tt>2</tt>.
     */
    public int getMinTermFreq() {
        return this.minTermFreq;
    }

    /**
     * The maximum number of query terms that will be included in any generated query. Defaults to <tt>25</tt>.
     */
    public MoreLikeThisRequest setMaxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * The maximum number of query terms that will be included in any generated query. Defaults to <tt>25</tt>.
     */
    public int getMaxQueryTerms() {
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
    public MoreLikeThisRequest setStopWords(String... stopWords) {
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
    public String[] getStopWords() {
        return this.stopWords;
    }

    /**
     * The frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public MoreLikeThisRequest setMinDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * The frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to <tt>5</tt>.
     */
    public int getMinDocFreq() {
        return this.minDocFreq;
    }

    /**
     * The maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisRequest setMaxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    /**
     * The maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     */
    public int getMaxDocFreq() {
        return this.maxDocFreq;
    }

    /**
     * The minimum word length below which words will be ignored. Defaults to <tt>0</tt>.
     */
    public MoreLikeThisRequest setMinWordLen(int minWordLen) {
        this.minWordLen = minWordLen;
        return this;
    }

    /**
     * The minimum word length below which words will be ignored. Defaults to <tt>0</tt>.
     */
    public int getMinWordLen() {
        return this.minWordLen;
    }

    /**
     * The maximum word length above which words will be ignored. Defaults to unbounded.
     */
    public MoreLikeThisRequest setMaxWordLen(int maxWordLen) {
        this.maxWordLen = maxWordLen;
        return this;
    }

    /**
     * The maximum word length above which words will be ignored. Defaults to unbounded.
     */
    public int getMaxWordLen() {
        return this.maxWordLen;
    }

    /**
     * The boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public MoreLikeThisRequest setBoostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * The boost factor to use when boosting terms. Defaults to <tt>1</tt>.
     */
    public float getBoostTerms() {
        return this.boostTerms;
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
    public MoreLikeThisRequest setSearchSource(SearchSourceBuilder sourceBuilder) {
        this.searchSource = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        this.searchSourceUnsafe = false;
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest setSearchSource(String searchSource) {
        this.searchSource = new BytesArray(searchSource);
        this.searchSourceUnsafe = false;
        return this;
    }

    public MoreLikeThisRequest setSearchSource(Map searchSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(searchSource);
            return setSearchSource(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + searchSource + "]", e);
        }
    }

    public MoreLikeThisRequest setSearchSource(XContentBuilder builder) {
        this.searchSource = builder.bytes();
        this.searchSourceUnsafe = false;
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest setSearchSource(byte[] searchSource) {
        return setSearchSource(searchSource, 0, searchSource.length, false);
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest setSearchSource(byte[] searchSource, int offset, int length, boolean unsafe) {
        return setSearchSource(new BytesArray(searchSource, offset, length), unsafe);
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public MoreLikeThisRequest setSearchSource(BytesReference searchSource, boolean unsafe) {
        this.searchSource = searchSource;
        this.searchSourceUnsafe = unsafe;
        return this;
    }

    /**
     * An optional search source request allowing to control the search request for the
     * more like this documents.
     */
    public BytesReference getSearchSource() {
        return this.searchSource;
    }

    public boolean isSearchSourceUnsafe() {
        return searchSourceUnsafe;
    }

    /**
     * The search type of the mlt search query.
     */
    public MoreLikeThisRequest setSearchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    /**
     * The search type of the mlt search query.
     */
    public MoreLikeThisRequest setSearchType(String searchType) throws ElasticSearchIllegalArgumentException {
        return setSearchType(SearchType.fromString(searchType));
    }

    /**
     * The search type of the mlt search query.
     */
    public SearchType getSearchType() {
        return this.searchType;
    }

    /**
     * The indices the resulting mlt query will run against. If not set, will run
     * against the index the document was fetched from.
     */
    public MoreLikeThisRequest setSearchIndices(String... searchIndices) {
        this.searchIndices = searchIndices;
        return this;
    }

    /**
     * The indices the resulting mlt query will run against. If not set, will run
     * against the index the document was fetched from.
     */
    public String[] getSearchIndices() {
        return this.searchIndices;
    }

    /**
     * The types the resulting mlt query will run against. If not set, will run
     * against the type of the document fetched.
     */
    public MoreLikeThisRequest setSearchTypes(String... searchTypes) {
        this.searchTypes = searchTypes;
        return this;
    }

    /**
     * The types the resulting mlt query will run against. If not set, will run
     * against the type of the document fetched.
     */
    public String[] getSearchTypes() {
        return this.searchTypes;
    }

    /**
     * Optional search query hint.
     */
    public MoreLikeThisRequest setSearchQueryHint(String searchQueryHint) {
        this.searchQueryHint = searchQueryHint;
        return this;
    }

    /**
     * Optional search query hint.
     */
    public String getSearchQueryHint() {
        return this.searchQueryHint;
    }

    /**
     * An optional search scroll request to be able to continue and scroll the search
     * operation.
     */
    public MoreLikeThisRequest setSearchScroll(Scroll searchScroll) {
        this.searchScroll = searchScroll;
        return this;
    }

    /**
     * An optional search scroll request to be able to continue and scroll the search
     * operation.
     */
    public Scroll getSearchScroll() {
        return this.searchScroll;
    }

    /**
     * The number of documents to return, defaults to 10.
     */
    public MoreLikeThisRequest setSearchSize(int size) {
        this.searchSize = size;
        return this;
    }

    public int getSearchSize() {
        return this.searchSize;
    }

    /**
     * From which search result set to return.
     */
    public MoreLikeThisRequest setSearchFrom(int from) {
        this.searchFrom = from;
        return this;
    }

    public int getSearchFrom() {
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
        minWordLen = in.readVInt();
        maxWordLen = in.readVInt();
        boostTerms = in.readFloat();
        searchType = SearchType.fromId(in.readByte());
        if (in.readBoolean()) {
            searchQueryHint = in.readString();
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
        out.writeVInt(minWordLen);
        out.writeVInt(maxWordLen);
        out.writeFloat(boostTerms);

        out.writeByte(searchType.id());
        if (searchQueryHint == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(searchQueryHint);
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
    }
}
