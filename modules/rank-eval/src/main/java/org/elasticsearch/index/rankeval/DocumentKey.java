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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class DocumentKey extends ToXContentToBytes implements Writeable {

    private String docId;
    private String type;
    private String index;

    void setIndex(String index) {
        this.index = index;
    }

    void setType(String type) {
        this.type = type;
    }

    void setDocId(String docId) {
        this.docId = docId;
    }

    public DocumentKey(String index, String type, String docId) {
        this.index = index;
        this.type = type;
        this.docId = docId;
    }

    public DocumentKey(StreamInput in) throws IOException {
        this.index = in.readString();
        this.type = in.readString();
        this.docId = in.readString();
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getDocID() {
        return docId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(type);
        out.writeString(docId);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DocumentKey other = (DocumentKey) obj;
        return Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(docId, other.docId);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(index, type, docId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RatedDocument.INDEX_FIELD.getPreferredName(), index);
        builder.field(RatedDocument.TYPE_FIELD.getPreferredName(), type);
        builder.field(RatedDocument.DOC_ID_FIELD.getPreferredName(), docId);
        builder.endObject();
        return builder;
    }
}
