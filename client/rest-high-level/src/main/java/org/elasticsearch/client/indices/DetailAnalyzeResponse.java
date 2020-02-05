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

package org.elasticsearch.client.indices;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DetailAnalyzeResponse {

    private final boolean customAnalyzer;
    private final AnalyzeTokenList analyzer;
    private final CharFilteredText[] charfilters;
    private final AnalyzeTokenList tokenizer;
    private final AnalyzeTokenList[] tokenfilters;

    private DetailAnalyzeResponse(boolean customAnalyzer,
                                 AnalyzeTokenList analyzer,
                                 List<CharFilteredText> charfilters,
                                 AnalyzeTokenList tokenizer,
                                 List<AnalyzeTokenList> tokenfilters) {
        this.customAnalyzer = customAnalyzer;
        this.analyzer = analyzer;
        this.charfilters = charfilters == null ? null : charfilters.toArray(new CharFilteredText[]{});
        this.tokenizer = tokenizer;
        this.tokenfilters = tokenfilters == null ? null : tokenfilters.toArray(new AnalyzeTokenList[]{});
    }

    public AnalyzeTokenList analyzer() {
        return this.analyzer;
    }

    public CharFilteredText[] charfilters() {
        return this.charfilters;
    }

    public AnalyzeTokenList tokenizer() {
        return tokenizer;
    }

    public AnalyzeTokenList[] tokenfilters() {
        return tokenfilters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DetailAnalyzeResponse that = (DetailAnalyzeResponse) o;
        return customAnalyzer == that.customAnalyzer &&
            Objects.equals(analyzer, that.analyzer) &&
            Arrays.equals(charfilters, that.charfilters) &&
            Objects.equals(tokenizer, that.tokenizer) &&
            Arrays.equals(tokenfilters, that.tokenfilters);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(customAnalyzer, analyzer, tokenizer);
        result = 31 * result + Arrays.hashCode(charfilters);
        result = 31 * result + Arrays.hashCode(tokenfilters);
        return result;
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<DetailAnalyzeResponse, Void> PARSER = new ConstructingObjectParser<>("detail",
        true, args -> new DetailAnalyzeResponse(
        (boolean) args[0],
        (AnalyzeTokenList) args[1],
        (List<CharFilteredText>)args[2],
        (AnalyzeTokenList) args[3],
        (List<AnalyzeTokenList>)args[4]));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("custom_analyzer"));
        PARSER.declareObject(optionalConstructorArg(), AnalyzeTokenList.PARSER, new ParseField("analyzer"));
        PARSER.declareObjectArray(optionalConstructorArg(), CharFilteredText.PARSER, new ParseField("charfilters"));
        PARSER.declareObject(optionalConstructorArg(), AnalyzeTokenList.PARSER, new ParseField("tokenizer"));
        PARSER.declareObjectArray(optionalConstructorArg(), AnalyzeTokenList.PARSER, new ParseField("tokenfilters"));
    }

    public static DetailAnalyzeResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static class AnalyzeTokenList {
        private final String name;
        private final AnalyzeResponse.AnalyzeToken[] tokens;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnalyzeTokenList that = (AnalyzeTokenList) o;
            return Objects.equals(name, that.name) &&
                Arrays.equals(tokens, that.tokens);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(tokens);
            return result;
        }

        public AnalyzeTokenList(String name, List<AnalyzeResponse.AnalyzeToken> tokens) {
            this.name = name;
            this.tokens = tokens.toArray(new AnalyzeResponse.AnalyzeToken[]{});
        }

        public String getName() {
            return name;
        }

        public AnalyzeResponse.AnalyzeToken[] getTokens() {
            return tokens;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AnalyzeTokenList, Void> PARSER = new ConstructingObjectParser<>("token_list",
            true, args -> new AnalyzeTokenList((String) args[0],
            (List<AnalyzeResponse.AnalyzeToken>)args[1]));

        static {
            PARSER.declareString(constructorArg(), new ParseField("name"));
            PARSER.declareObjectArray(constructorArg(), (p, c) -> AnalyzeResponse.AnalyzeToken.fromXContent(p),
                new ParseField("tokens"));
        }

        public static AnalyzeTokenList fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

    }

    public static class CharFilteredText {
        private final String name;
        private final String[] texts;

        CharFilteredText(String name, String[] texts) {
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

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<CharFilteredText, Void> PARSER = new ConstructingObjectParser<>("char_filtered_text",
            true, args -> new CharFilteredText((String) args[0], ((List<String>) args[1]).toArray(new String[0])));

        static {
            PARSER.declareString(constructorArg(), new ParseField("name"));
            PARSER.declareStringArray(constructorArg(), new ParseField("filtered_text"));
        }

        public static CharFilteredText fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CharFilteredText that = (CharFilteredText) o;
            return Objects.equals(name, that.name) &&
                Arrays.equals(texts, that.texts);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(texts);
            return result;
        }
    }
}
