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

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * The response of a get action.
 *
 * @see GetRequest
 * @see org.elasticsearch.client.Client#get(GetRequest)
 */
public class GetResponse extends ActionResponse implements Iterable<GetField>, ToXContent {

    private GetResult getResult;

    GetResponse() {
    }

    public GetResponse(GetResult getResult) {
        this.getResult = getResult;
    }

    /**
     * Does the document exists.
     */
    public boolean isExists() {
        return getResult.isExists();
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return getResult.getIndex();
    }

    /**
     * The type of the document.
     */
    public String getType() {
        return getResult.getType();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return getResult.getId();
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return getResult.getVersion();
    }

    /**
     * The source of the document if exists.
     */
    public byte[] getSourceAsBytes() {
        return getResult.source();
    }

    /**
     * Returns the internal source bytes, as they are returned without munging (for example,
     * might still be compressed).
     */
    public BytesReference getSourceInternal() {
        return getResult.internalSourceRef();
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference getSourceAsBytesRef() {
        return getResult.sourceRef();
    }

    /**
     * Is the source empty (not available) or not.
     */
    public boolean isSourceEmpty() {
        return getResult.isSourceEmpty();
    }

    /**
     * The source of the document (as a string).
     */
    public String getSourceAsString() {
        return getResult.sourceAsString();
    }

    /**
     * The source of the document (As a map).
     */
    @SuppressWarnings({"unchecked"})
    public Map<String, Object> getSourceAsMap() throws ElasticsearchParseException {
        return getResult.sourceAsMap();
    }

    public Map<String, Object> getSource() {
        return getResult.getSource();
    }

    public Map<String, GetField> getFields() {
        return getResult.getFields();
    }

    public GetField getField(String name) {
        return getResult.field(name);
    }

    @Override
    public Iterator<GetField> iterator() {
        return getResult.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return getResult.toXContent(builder, params);
    }

    public static GetResponse readGetResponse(StreamInput in) throws IOException {
        GetResponse result = new GetResponse();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        getResult = GetResult.readGetResult(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        getResult.writeTo(out);
    }
}
