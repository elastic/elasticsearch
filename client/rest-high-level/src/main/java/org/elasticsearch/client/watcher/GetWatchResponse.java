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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class GetWatchResponse {
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private final WatchStatus status;

    private final BytesReference source;
    private final XContentType xContentType;

    /**
     * Ctor for missing watch
     */
    public GetWatchResponse(String id) {
        this(id, Versions.NOT_FOUND, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, null, null, null);
    }

    public GetWatchResponse(String id, long version, long seqNo, long primaryTerm, WatchStatus status, 
                            BytesReference source, XContentType xContentType) {
        this.id = id;
        this.version = version;
        this.status = status;
        this.source = source;
        this.xContentType = xContentType;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
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

    public boolean isFound() {
        return version != Versions.NOT_FOUND;
    }

    public WatchStatus getStatus() {
        return status;
    }

    /**
     * Returns the {@link XContentType} of the source
     */
    public XContentType getContentType() {
        return xContentType;
    }

    /**
     * Returns the serialized watch
     */
    public BytesReference getSource() {
        return source;
    }

    /**
     * Returns the source as a map
     */
    public Map<String, Object> getSourceAsMap() {
        return source == null ? null : XContentHelper.convertToMap(source, false, getContentType()).v2();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetWatchResponse that = (GetWatchResponse) o;
        return version == that.version &&
            Objects.equals(id, that.id) &&
            Objects.equals(status, that.status) &&
            Objects.equals(xContentType, that.xContentType) &&
            Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, status, source, version);
    }

    private static final ParseField ID_FIELD = new ParseField("_id");
    private static final ParseField FOUND_FIELD = new ParseField("found");
    private static final ParseField VERSION_FIELD = new ParseField("_version");
    private static final ParseField SEQ_NO_FIELD = new ParseField("_seq_no");
    private static final ParseField PRIMARY_TERM_FIELD = new ParseField("_primary_term");
    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final ParseField WATCH_FIELD = new ParseField("watch");

    private static ConstructingObjectParser<GetWatchResponse, Void> PARSER =
        new ConstructingObjectParser<>("get_watch_response", true,
            a -> {
                boolean isFound = (boolean) a[1];
                if (isFound) {
                    XContentBuilder builder = (XContentBuilder) a[6];
                    BytesReference source = BytesReference.bytes(builder);
                    return new GetWatchResponse((String) a[0], (long) a[2], (long) a[3], (long) a[4], (WatchStatus) a[5], 
                        source, builder.contentType());
                } else {
                    return new GetWatchResponse((String) a[0]);
                }
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), FOUND_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), SEQ_NO_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PRIMARY_TERM_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            (parser, context) -> WatchStatus.parse(parser), STATUS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            (parser, context) -> {
                try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                    builder.copyCurrentStructure(parser);
                    return builder;
                }
            }, WATCH_FIELD);
    }

    public static GetWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
