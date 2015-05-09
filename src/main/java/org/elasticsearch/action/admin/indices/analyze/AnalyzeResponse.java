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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

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
        private int flags;
        private int increment;
        private String payload;
        private boolean keyword;
        private boolean alldata;

        AnalyzeToken() {
        }

        public AnalyzeToken(String term, int position, int startOffset, int endOffset, String type) {
            this(false,term,position,startOffset,endOffset,type,0,0,false,""); //keep initial sygnaure for back compatibility
        }

        public AnalyzeToken(boolean alldata,String term, int position, int startOffset, int endOffset, String type,
                            int flags,int increment,boolean keyword, String payload) {
            this.alldata = alldata;
            this.term = term;
            this.position = position;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.flags = flags;
            this.type = type;
            this.increment = increment;
            this.payload =payload;
            this.keyword = keyword;
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

        public int getFlags() { return  this.flags; }
        public boolean isAlldata() { return  this.alldata; }

        public int getIncrement() { return  this.increment; }
        public boolean getKeyword() { return  this.keyword; }
        public String getPayload() { return  this.payload; }

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
            alldata = in.readBoolean();
            term = in.readString();
            startOffset = in.readInt();
            endOffset = in.readInt();
            position = in.readVInt();
            type = in.readOptionalString();
            flags = in.readInt();
            increment = in.readInt();
            keyword = in.readBoolean();
            payload = in.readOptionalString();

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(alldata);
            out.writeString(term);
            out.writeInt(startOffset);
            out.writeInt(endOffset);
            out.writeVInt(position);
            out.writeOptionalString(type);
            out.writeInt(flags);
            out.writeInt(increment);
            out.writeBoolean(keyword);
            out.writeOptionalString(payload);
        }
    }

    private List<AnalyzeToken> tokens;

    AnalyzeResponse() {
    }

    public AnalyzeResponse(List<AnalyzeToken> tokens) {
        this.tokens = tokens;
    }

    public List<AnalyzeToken> getTokens() {
        return this.tokens;
    }

    @Override
    public Iterator<AnalyzeToken> iterator() {
        return tokens.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.TOKENS);
        for (AnalyzeToken token : tokens) {
            builder.startObject();
            builder.field(Fields.TOKEN, token.getTerm());
            builder.field(Fields.START_OFFSET, token.getStartOffset());
            builder.field(Fields.END_OFFSET, token.getEndOffset());
            builder.field(Fields.TYPE, token.getType());
            builder.field(Fields.POSITION, token.getPosition());
            if(token.isAlldata()) {
                builder.field(Fields.FLAGS, token.getFlags());
                builder.field(Fields.INCREMENT, token.getIncrement());
                builder.field(Fields.KEYWORD, token.getKeyword());
                builder.field(Fields.PAYLOAD, token.getPayload());
            }
            builder.endObject();
        }
        builder.endArray();
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(tokens.size());
        for (AnalyzeToken token : tokens) {
            token.writeTo(out);
        }
    }

    static final class Fields {
        static final XContentBuilderString TOKENS = new XContentBuilderString("tokens");
        static final XContentBuilderString TOKEN = new XContentBuilderString("token");
        static final XContentBuilderString START_OFFSET = new XContentBuilderString("start_offset");
        static final XContentBuilderString END_OFFSET = new XContentBuilderString("end_offset");
        static final XContentBuilderString TYPE = new XContentBuilderString("type");
        static final XContentBuilderString POSITION = new XContentBuilderString("position");
        static final XContentBuilderString FLAGS = new XContentBuilderString("flags");
        static final XContentBuilderString INCREMENT = new XContentBuilderString("increment");
        static final XContentBuilderString KEYWORD = new XContentBuilderString("keyword");
        static final XContentBuilderString PAYLOAD = new XContentBuilderString("payload");
    }
}
