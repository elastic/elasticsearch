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


import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class DetailAnalyzeResponse implements Streamable, ToXContent {

    DetailAnalyzeResponse() {
    }

    private boolean customAnalyzer = false;
    private AnalyzeTokenList analyzer;
    private CharFilteredText[] charfilters;
    private AnalyzeTokenList tokenizer;
    private AnalyzeTokenList[] tokenfilters;

    public DetailAnalyzeResponse(AnalyzeTokenList analyzer) {
        this(false, analyzer, null, null, null);
    }

    public DetailAnalyzeResponse(CharFilteredText[] charfilters, AnalyzeTokenList tokenizer, AnalyzeTokenList[] tokenfilters) {
        this(true, null, charfilters, tokenizer, tokenfilters);
    }

    public DetailAnalyzeResponse(boolean customAnalyzer,
                                 AnalyzeTokenList analyzer,
                                 CharFilteredText[] charfilters,
                                 AnalyzeTokenList tokenizer,
                                 AnalyzeTokenList[] tokenfilters) {
        this.customAnalyzer = customAnalyzer;
        this.analyzer = analyzer;
        this.charfilters = charfilters;
        this.tokenizer = tokenizer;
        this.tokenfilters = tokenfilters;
    }

    public AnalyzeTokenList analyzer() {
        return this.analyzer;
    }

    public DetailAnalyzeResponse analyzer(AnalyzeTokenList analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public CharFilteredText[] charfilters() {
        return this.charfilters;
    }

    public DetailAnalyzeResponse charfilters(CharFilteredText[] charfilters) {
        this.charfilters = charfilters;
        return this;
    }

    public AnalyzeTokenList tokenizer() {
        return tokenizer;
    }

    public DetailAnalyzeResponse tokenizer(AnalyzeTokenList tokenizer) {
        this.tokenizer = tokenizer;
        return this;
    }

    public AnalyzeTokenList[] tokenfilters() {
        return tokenfilters;
    }

    public DetailAnalyzeResponse tokenfilters(AnalyzeTokenList[] tokenfilters) {
        this.tokenfilters = tokenfilters;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.CUSTOM_ANALYZER, customAnalyzer);

        if (analyzer != null) {
            builder.startObject(Fields.ANALYZER);
            analyzer.toXContentWithoutObject(builder, params);
            builder.endObject();
        }

        if (charfilters != null) {
            builder.startArray(Fields.CHARFILTERS);
            for (CharFilteredText charfilter : charfilters) {
                charfilter.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (tokenizer != null) {
            builder.startObject(Fields.TOKENIZER);
            tokenizer.toXContentWithoutObject(builder, params);
            builder.endObject();
        }

        if (tokenfilters != null) {
            builder.startArray(Fields.TOKENFILTERS);
            for (AnalyzeTokenList tokenfilter : tokenfilters) {
                tokenfilter.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }

    static final class Fields {
        static final String NAME = "name";
        static final String FILTERED_TEXT = "filtered_text";
        static final String CUSTOM_ANALYZER = "custom_analyzer";
        static final String ANALYZER = "analyzer";
        static final String CHARFILTERS = "charfilters";
        static final String TOKENIZER = "tokenizer";
        static final String TOKENFILTERS = "tokenfilters";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.customAnalyzer = in.readBoolean();
        if (customAnalyzer) {
            tokenizer = AnalyzeTokenList.readAnalyzeTokenList(in);
            int size = in.readVInt();
            if (size > 0) {
                charfilters = new CharFilteredText[size];
                for (int i = 0; i < size; i++) {
                    charfilters[i] = CharFilteredText.readCharFilteredText(in);
                }
            }
            size = in.readVInt();
            if (size > 0) {
                tokenfilters = new AnalyzeTokenList[size];
                for (int i = 0; i < size; i++) {
                    tokenfilters[i] = AnalyzeTokenList.readAnalyzeTokenList(in);
                }
            }
        } else {
            analyzer = AnalyzeTokenList.readAnalyzeTokenList(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(customAnalyzer);
        if (customAnalyzer) {
            tokenizer.writeTo(out);
            if (charfilters != null) {
                out.writeVInt(charfilters.length);
                for (CharFilteredText charfilter : charfilters) {
                    charfilter.writeTo(out);
                }
            } else {
                out.writeVInt(0);
            }
            if (tokenfilters != null) {
                out.writeVInt(tokenfilters.length);
                for (AnalyzeTokenList tokenfilter : tokenfilters) {
                    tokenfilter.writeTo(out);
                }
            } else {
                out.writeVInt(0);
            }
        } else {
            analyzer.writeTo(out);
        }
    }

    public static class AnalyzeTokenList implements Streamable, ToXContent {
        private String name;
        private AnalyzeResponse.AnalyzeToken[] tokens;

        AnalyzeTokenList() {
        }

        public AnalyzeTokenList(String name, AnalyzeResponse.AnalyzeToken[] tokens) {
            this.name = name;
            this.tokens = tokens;
        }

        public String getName() {
            return name;
        }

        public AnalyzeResponse.AnalyzeToken[] getTokens() {
            return tokens;
        }

        public static AnalyzeTokenList readAnalyzeTokenList(StreamInput in) throws IOException {
            AnalyzeTokenList list = new AnalyzeTokenList();
            list.readFrom(in);
            return list;
        }

        public XContentBuilder toXContentWithoutObject(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.NAME, this.name);
            builder.startArray(AnalyzeResponse.Fields.TOKENS);
            for (AnalyzeResponse.AnalyzeToken token : tokens) {
                token.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, this.name);
            builder.startArray(AnalyzeResponse.Fields.TOKENS);
            for (AnalyzeResponse.AnalyzeToken token : tokens) {
                token.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            int size = in.readVInt();
            if (size > 0) {
                tokens = new AnalyzeResponse.AnalyzeToken[size];
                for (int i = 0; i < size; i++) {
                    tokens[i] = AnalyzeResponse.AnalyzeToken.readAnalyzeToken(in);
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            if (tokens != null) {
                out.writeVInt(tokens.length);
                for (AnalyzeResponse.AnalyzeToken token : tokens) {
                    token.writeTo(out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }

    public static class CharFilteredText implements Streamable, ToXContent {
        private String name;
        private String[] texts;
        CharFilteredText() {
        }

        public CharFilteredText(String name, String[] texts) {
            this.name = name;
            if (texts != null) {
                this.texts = texts;
            } else {
                this.texts = Strings.EMPTY_ARRAY;
            }
        }

        public String getName() {
            return name;
        }

        public String[] getTexts() {
            return texts;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.field(Fields.FILTERED_TEXT, texts);
            builder.endObject();
            return builder;
        }

        public static CharFilteredText readCharFilteredText(StreamInput in) throws IOException {
            CharFilteredText text = new CharFilteredText();
            text.readFrom(in);
            return text;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            texts = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeStringArray(texts);
        }
    }
}
