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

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
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
public class GetResponse implements ActionResponse, Streamable, Iterable<GetField>, ToXContent {

    private GetResult getResult;

    GetResponse() {
    }

    GetResponse(GetResult getResult) {
        this.getResult = getResult;
    }

    /**
     * Does the document exists.
     */
    public boolean exists() {
        return getResult.exists();
    }

    /**
     * Does the document exists.
     */
    public boolean isExists() {
        return exists();
    }

    /**
     * The index the document was fetched from.
     */
    public String index() {
        return getResult.index();
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return index();
    }

    /**
     * The type of the document.
     */
    public String type() {
        return getResult.type();
    }

    /**
     * The type of the document.
     */
    public String getType() {
        return type();
    }

    /**
     * The id of the document.
     */
    public String id() {
        return getResult.id();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id();
    }

    /**
     * The version of the doc.
     */
    public long version() {
        return getResult.version();
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return version();
    }

    /**
     * The source of the document if exists.
     */
    public byte[] source() {
        return getResult.source();
    }

    /**
     * The source of the document if exists.
     */
    public byte[] getSourceAsBytes() {
        return source();
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference sourceRef() {
        return getResult.sourceRef();
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference getSourceAsBytesRef() {
        return sourceRef();
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
    public String sourceAsString() {
        return getResult.sourceAsString();
    }

    public String getSourceAsString() {
        return sourceAsString();
    }

    /**
     * The source of the document (As a map).
     */
    @SuppressWarnings({"unchecked"})
    public Map<String, Object> sourceAsMap() throws ElasticSearchParseException {
        return getResult.sourceAsMap();
    }

    public Map<String, Object> getSource() {
        return getResult.getSource();
    }

    public Map<String, GetField> fields() {
        return getResult.fields();
    }

    public Map<String, GetField> getFields() {
        return fields();
    }

    public GetField field(String name) {
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        getResult = GetResult.readGetResult(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        getResult.writeTo(out);
    }
}
