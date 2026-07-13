/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the outcome of a search query in the connectors and sync job index, encapsulating the search result.
 * It includes a raw byte reference to the result which can be deserialized into a {@link Connector} or {@link ConnectorSyncJob} object,
 * and a result map for returning the data without strict deserialization.
 */
public class ConnectorsAPISearchResult implements Writeable, ToXContentObject {

    private final BytesReference resultBytes;
    private final Map<String, Object> resultMap;
    private final String docId;

    protected ConnectorsAPISearchResult(BytesReference resultBytes, Map<String, Object> resultMap, String id) {
        this.resultBytes = resultBytes;
        this.resultMap = resultMap;
        this.docId = id;
    }

    public ConnectorsAPISearchResult(StreamInput in) throws IOException {
        this.resultBytes = in.readBytesReference();
        this.resultMap = in.readGenericMap();
        this.docId = in.readString();
    }

    public BytesReference getSourceRef() {
        return resultBytes;
    }

    public Map<String, Object> getResultMap() {
        return resultMap;
    }

    public String getDocId() {
        return docId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("id", docId);
            builder.mapContents(resultMap);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(resultBytes);
        out.writeGenericMap(resultMap);
        out.writeString(docId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorsAPISearchResult that = (ConnectorsAPISearchResult) o;
        return Objects.equals(resultBytes, that.resultBytes)
            && Objects.equals(resultMap, that.resultMap)
            && Objects.equals(docId, that.docId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultBytes, resultMap, docId);
    }
}
