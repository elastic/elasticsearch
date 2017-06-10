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
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AnalyzeResponse extends ActionResponse implements Iterable<AnalyzeResponse.AnalyzeToken>, ToXContentObject {

    public static class AnalyzeToken implements Streamable, ToXContentObject {
        private String term;
        private int startOffset;
        private int endOffset;
        private int position;
        private int positionLength = 1;
        private Map<String, Object> attributes;
        private String type;

        AnalyzeToken() {
        }

        public AnalyzeToken(String term, int position, int startOffset, int endOffset, int positionLength,
                            String type, Map<String, Object> attributes) {
            this.term = term;
            this.position = position;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.positionLength = positionLength;
            this.type = type;
            this.attributes = attributes;
        }

        public String getTerm() {
            return this.term;
        }

        public int getStartOffset() {
            return this.startOffset;
        }

        public int getEndOffset() {
            return this.endOffset;
        }

        public int getPosition() {
            return this.position;
        }

        public int getPositionLength() {
            return this.positionLength;
        }

        public String getType() {
            return this.type;
        }

        public Map<String, Object> getAttributes(){
            return this.attributes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.TOKEN, term);
            builder.field(Fields.START_OFFSET, startOffset);
            builder.field(Fields.END_OFFSET, endOffset);
            builder.field(Fields.TYPE, type);
            builder.field(Fields.POSITION, position);
            if (positionLength > 1) {
                builder.field(Fields.POSITION_LENGTH, positionLength);
            }
            if (attributes != null && !attributes.isEmpty()) {
                for (Map.Entry<String, Object> entity : attributes.entrySet()) {
                    builder.field(entity.getKey(), entity.getValue());
                }
            }
            builder.endObject();
            return builder;
        }

        public static AnalyzeToken readAnalyzeToken(StreamInput in) throws IOException {
            AnalyzeToken analyzeToken = new AnalyzeToken();
            analyzeToken.readFrom(in);
            return analyzeToken;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            term = in.readString();
            startOffset = in.readInt();
            endOffset = in.readInt();
            position = in.readVInt();
            if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
                Integer len = in.readOptionalVInt();
                if (len != null) {
                    positionLength = len;
                } else {
                    positionLength = 1;
                }
            }
            type = in.readOptionalString();
            attributes = (Map<String, Object>) in.readGenericValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(term);
            out.writeInt(startOffset);
            out.writeInt(endOffset);
            out.writeVInt(position);
            if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
                out.writeOptionalVInt(positionLength > 1 ? positionLength : null);
            }
            out.writeOptionalString(type);
            out.writeGenericValue(attributes);
        }
    }

    private DetailAnalyzeResponse detail;

    private List<AnalyzeToken> tokens;

    AnalyzeResponse() {
    }

    public AnalyzeResponse(List<AnalyzeToken> tokens, DetailAnalyzeResponse detail) {
        this.tokens = tokens;
        this.detail = detail;
    }

    public List<AnalyzeToken> getTokens() {
        return this.tokens;
    }

    public DetailAnalyzeResponse detail() {
        return this.detail;
    }

    @Override
    public Iterator<AnalyzeToken> iterator() {
        return tokens.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (tokens != null) {
            builder.startArray(Fields.TOKENS);
            for (AnalyzeToken token : tokens) {
                token.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (detail != null) {
            builder.startObject(Fields.DETAIL);
            detail.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        tokens = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            tokens.add(AnalyzeToken.readAnalyzeToken(in));
        }
        detail = in.readOptionalStreamable(DetailAnalyzeResponse::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (tokens != null) {
            out.writeVInt(tokens.size());
            for (AnalyzeToken token : tokens) {
                token.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeOptionalStreamable(detail);
    }

    static final class Fields {
        static final String TOKENS = "tokens";
        static final String TOKEN = "token";
        static final String START_OFFSET = "start_offset";
        static final String END_OFFSET = "end_offset";
        static final String TYPE = "type";
        static final String POSITION = "position";
        static final String POSITION_LENGTH = "positionLength";
        static final String DETAIL = "detail";
    }
}
