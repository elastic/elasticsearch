/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class AnalyzeResponse extends ActionResponse implements Iterable<AnalyzeResponse.AnalyzeToken>, ToXContent {

    public static class AnalyzeToken implements Streamable {
        private String term;
        private int startOffset;
        private int endOffset;
        private int position;
        private String type;

        AnalyzeToken() {
        }

        public AnalyzeToken(String term, int position, int startOffset, int endOffset, String type) {
            this.term = term;
            this.position = position;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.type = type;
        }

        public String term() {
            return this.term;
        }

        public String getTerm() {
            return term();
        }

        public int startOffset() {
            return this.startOffset;
        }

        public int getStartOffset() {
            return startOffset();
        }

        public int endOffset() {
            return this.endOffset;
        }

        public int getEndOffset() {
            return endOffset();
        }

        public int position() {
            return this.position;
        }

        public int getPosition() {
            return position();
        }

        public String type() {
            return this.type;
        }

        public String getType() {
            return this.type;
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
            type = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(term);
            out.writeInt(startOffset);
            out.writeInt(endOffset);
            out.writeVInt(position);
            out.writeOptionalString(type);
        }
    }

    private List<AnalyzeToken> tokens;

    AnalyzeResponse() {
    }

    public AnalyzeResponse(List<AnalyzeToken> tokens) {
        this.tokens = tokens;
    }

    public List<AnalyzeToken> tokens() {
        return this.tokens;
    }

    public List<AnalyzeToken> getTokens() {
        return tokens();
    }

    @Override
    public Iterator<AnalyzeToken> iterator() {
        return tokens.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        String format = params.param("format", "detailed");
        if ("detailed".equals(format)) {
            builder.startArray("tokens");
            for (AnalyzeToken token : tokens) {
                builder.startObject();
                builder.field("token", token.term());
                builder.field("start_offset", token.startOffset());
                builder.field("end_offset", token.endOffset());
                builder.field("type", token.type());
                builder.field("position", token.position());
                builder.endObject();
            }
            builder.endArray();
        } else if ("text".equals(format)) {
            StringBuilder sb = new StringBuilder();
            int lastPosition = 0;
            for (AnalyzeToken token : tokens) {
                if (lastPosition != token.position()) {
                    if (lastPosition != 0) {
                        sb.append("\n").append(token.position()).append(": \n");
                    }
                    lastPosition = token.position();
                }
                sb.append('[')
                        .append(token.term()).append(":")
                        .append(token.startOffset()).append("->").append(token.endOffset()).append(":")
                        .append(token.type())
                        .append("]\n");
            }
            builder.field("tokens", sb);
        }
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        tokens = new ArrayList<AnalyzeToken>(size);
        for (int i = 0; i < size; i++) {
            tokens.add(AnalyzeToken.readAnalyzeToken(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(tokens.size());
        for (AnalyzeToken token : tokens) {
            token.writeTo(out);
        }
    }
}
