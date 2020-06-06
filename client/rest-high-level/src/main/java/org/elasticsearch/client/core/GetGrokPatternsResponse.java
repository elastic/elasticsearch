/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class GetGrokPatternsResponse {
    private static final String GROK_PATTERN_KEY = "[A-Z_0-9]+";
    private static final String GROK_PATTERN_VALUE =
        "%\\{" +
            "(?<name>" +
            "(?<pattern>[A-z0-9]+)" +
            "(?::(?<subname>[[:alnum:]@\\[\\]_:.-]+))?" +
            ")" +
            "(?:=(?<definition>" +
            "(?:[^{}]+|\\.+)+" +
            ")" +
            ")?" + "\\}";
    static final ParseField PATTERNS = new ParseField("patterns");
    static final ParseField PATTERN_KEY = new ParseField(GROK_PATTERN_KEY);
    static final ParseField PATTERN_VALUE = new ParseField(GROK_PATTERN_VALUE);

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetGrokPatternsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "patterns",
            true,
            args -> new GetGrokPatternsResponse(new TreeMap<>(
                ((List<Map.Entry<String, String>>) args[0])
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))));

    private static final ConstructingObjectParser<Map.Entry<String, String>, Void> PATTERNS_PARSER =
        new ConstructingObjectParser<>(
            "patterns",
            true,
            args -> new AbstractMap.SimpleEntry<>((String) args[0], (String) args[1]));

    static{
        PATTERNS_PARSER.declareString(ConstructingObjectParser.constructorArg(), PATTERN_KEY);
        PATTERNS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), PATTERN_VALUE);

        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), PATTERNS_PARSER,
            PATTERNS);
    }
    private final Map<String, String> grokPatterns;

    GetGrokPatternsResponse(Map<String, String> grokPatterns) {
        this.grokPatterns = grokPatterns;
    }

    public Map<String, String> getGrokPatterns() {
        return grokPatterns;
    }

    public static GetGrokPatternsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return grokPatterns.toString();
    }
}
