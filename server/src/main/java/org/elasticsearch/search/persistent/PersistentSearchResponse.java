/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.persistent.PersistentSearchStorageService.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.search.persistent.PersistentSearchStorageService.ID_FIELD;
import static org.elasticsearch.search.persistent.PersistentSearchStorageService.REDUCED_SHARDS_INDEX_FIELD;
import static org.elasticsearch.search.persistent.PersistentSearchStorageService.RESPONSE_FIELD;
import static org.elasticsearch.search.persistent.PersistentSearchStorageService.SEARCH_ID_FIELD;

public class PersistentSearchResponse extends ActionResponse implements ToXContentObject {
    private final String id;
    private final String searchId;
    private final SearchResponse searchResponse;
    private final long expirationTime;
    // TODO: Optimize this?
    private final List<Integer> reducedShardsIndex;
    private final long version;

    public PersistentSearchResponse(String id,
                                    String searchId,
                                    SearchResponse searchResponse,
                                    long expirationTime,
                                    List<Integer> reducedShardsIndex,
                                    long version) {
        this.id = id;
        this.searchId = searchId;
        this.searchResponse = searchResponse;
        this.expirationTime = expirationTime;
        this.reducedShardsIndex = List.copyOf(reducedShardsIndex);
        this.version = version;
    }

    public PersistentSearchResponse(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.searchId = in.readString();
        this.searchResponse = new SearchResponse(in);
        this.expirationTime = in.readLong();
        this.reducedShardsIndex = in.readList(StreamInput::readInt);
        this.version = in.readLong();
    }

    public String getId() {
        return id;
    }

    public String getSearchId() {
        return searchId;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public List<Integer> getReducedShardIndices() {
        return reducedShardsIndex;
    }

    public long getVersion() {
        return version;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public static PersistentSearchResponse fromXContent(Map<String, Object> source,
                                                        long version,
                                                        NamedWriteableRegistry namedWriteableRegistry) throws Exception {
        final String id = (String) source.get(ID_FIELD);
        if (id == null) {
            throw invalidDoc(ID_FIELD);
        }

        final String searchId = (String) source.get(SEARCH_ID_FIELD);
        if (searchId == null) {
            throw invalidDoc(SEARCH_ID_FIELD);
        }

        final Long expirationTime = (Long) source.get(EXPIRATION_TIME_FIELD);
        if (expirationTime == null) {
            throw invalidDoc(EXPIRATION_TIME_FIELD);
        }

        final List<Integer> reducedShardIndices = (List<Integer>) source.get(REDUCED_SHARDS_INDEX_FIELD);
        if (reducedShardIndices == null) {
            throw invalidDoc(REDUCED_SHARDS_INDEX_FIELD);
        }

        final String encodedSearchResponse = (String) source.get(RESPONSE_FIELD);
        if (encodedSearchResponse == null) {
            throw invalidDoc(RESPONSE_FIELD);
        }
        final byte[] jsonSearchResponse = Base64.getDecoder().decode(encodedSearchResponse);
        final BytesReference encodedQuerySearchResult = BytesReference.fromByteBuffer(ByteBuffer.wrap(jsonSearchResponse));
        SearchResponse searchResponse = decodeSearchResponse(encodedQuerySearchResult, namedWriteableRegistry);

        return new PersistentSearchResponse(id, searchId, searchResponse, expirationTime, reducedShardIndices, version);
    }

    private static IllegalArgumentException invalidDoc(String missingField) {
        return new IllegalArgumentException("Invalid document, '" + missingField + "' field is missing");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(searchId);
        searchResponse.writeTo(out);
        out.writeLong(expirationTime);
        out.writeCollection(reducedShardsIndex, StreamOutput::write);
        out.writeLong(version);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ID_FIELD, id);
            builder.field(SEARCH_ID_FIELD, id);
            builder.field(RESPONSE_FIELD, encodeSearchResponse(searchResponse));
            builder.field(EXPIRATION_TIME_FIELD, System.currentTimeMillis());
            builder.field(REDUCED_SHARDS_INDEX_FIELD, reducedShardsIndex);
        }
        builder.endObject();
        return builder;
    }

    private BytesReference encodeSearchResponse(SearchResponse searchResponse) throws IOException {
        // TODO: introduce circuit breaker?
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            searchResponse.writeTo(out);
            return out.bytes();
        }
    }

    private static SearchResponse decodeSearchResponse(BytesReference encodedQuerySearchResult,
                                                       NamedWriteableRegistry namedWriteableRegistry) throws Exception {
        try (StreamInput in = new NamedWriteableAwareStreamInput(encodedQuerySearchResult.streamInput(), namedWriteableRegistry)) {
            return new SearchResponse(in);
        }
    }
}
