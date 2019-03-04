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
package org.elasticsearch.client.watcher;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.Objects;

public class PutWatchResponse {

    private static final ObjectParser<PutWatchResponse, Void> PARSER
        = new ObjectParser<>("x_pack_put_watch_response", true, PutWatchResponse::new);

    static {
        PARSER.declareString(PutWatchResponse::setId, new ParseField("_id"));
        PARSER.declareLong(PutWatchResponse::setSeqNo, new ParseField("_seq_no"));
        PARSER.declareLong(PutWatchResponse::setPrimaryTerm, new ParseField("_primary_term"));
        PARSER.declareLong(PutWatchResponse::setVersion, new ParseField("_version"));
        PARSER.declareBoolean(PutWatchResponse::setCreated, new ParseField("created"));
    }

    private String id;
    private long version;
    private long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    private boolean created;

    public PutWatchResponse() {
    }

    public PutWatchResponse(String id, long version, long seqNo, long primaryTerm, boolean created) {
        this.id = id;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.created = created;
    }

    private void setId(String id) {
        this.id = id;
    }

    private void setVersion(long version) {
        this.version = version;
    }

    private void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    private void setPrimaryTerm(long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    private void setCreated(boolean created) {
        this.created = created;
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PutWatchResponse that = (PutWatchResponse) o;

        return Objects.equals(id, that.id) && Objects.equals(version, that.version)
            && Objects.equals(seqNo, that.seqNo)
            && Objects.equals(primaryTerm, that.primaryTerm) && Objects.equals(created, that.created);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, seqNo, primaryTerm, created);
    }

    public static PutWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
