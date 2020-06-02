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

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Class describing how to group data
 */
public class GroupConfig implements ToXContentObject {

    private final Map<String, SingleGroupSource> groups;

    /**
     * Leniently parse a {@code GroupConfig}.
     * Parsing is lenient in that unknown fields in the root of the
     * object are ignored. Unknown group types {@link SingleGroupSource.Type}
     * will cause a parsing error.
     *
     * @param parser The XContent parser
     * @return The parsed object
     * @throws IOException On parsing error
     */
    public static GroupConfig fromXContent(final XContentParser parser) throws IOException {
        LinkedHashMap<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // be parsing friendly, whether the token needs to be advanced or not (similar to what ObjectParser does)
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Failed to parse object: Expected START_OBJECT but was: " + token);
            }
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                // leniently skip over key-value and array fields in the root of the object
                if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
                continue;
            }

            String destinationFieldName = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            String groupType = parser.currentName();

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                // need to consume up to dest field end obj
                consumeUntilEndObject(parser, 1);
                continue;
            }

            SingleGroupSource groupSource = null;
            switch (groupType) {
                case "terms":
                    groupSource = TermsGroupSource.fromXContent(parser);
                    break;
                case "histogram":
                    groupSource = HistogramGroupSource.fromXContent(parser);
                    break;
                case "date_histogram":
                    groupSource = DateHistogramGroupSource.fromXContent(parser);
                    break;
                case "geotile_grid":
                    groupSource = GeoTileGroupSource.fromXContent(parser);
                    break;
                default:
                    // not a valid group source. Consume up to the dest field end object
                    consumeUntilEndObject(parser, 2);
            }

            if (groupSource != null) {
                groups.put(destinationFieldName, groupSource);
                // destination field end_object
                parser.nextToken();
            }
        }
        return new GroupConfig(groups);
    }

    /**
     * Consume tokens from the parser until {@code endObjectCount} of end object
     * tokens have been read. Nested objects that start and end inside the current
     * field are skipped and do contribute to the end object count.
     * @param parser The XContent parser
     * @param endObjectCount Number of end object tokens to consume
     * @throws IOException On parsing error
     */
    private static void consumeUntilEndObject(XContentParser parser, int endObjectCount) throws IOException {
        do {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                endObjectCount++;
            } else if (token == XContentParser.Token.END_OBJECT) {
                endObjectCount--;
            }
        } while (endObjectCount != 0);
    }

    GroupConfig(Map<String, SingleGroupSource> groups) {
        this.groups = groups;
    }

    public Map <String, SingleGroupSource> getGroups() {
        return groups;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, SingleGroupSource> entry : groups.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field(entry.getValue().getType().value(), entry.getValue());
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GroupConfig that = (GroupConfig) other;

        return Objects.equals(this.groups, that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<String, SingleGroupSource> groups = new HashMap<>();

        /**
         * Add a new grouping to the builder
         * @param name The name of the resulting grouped field
         * @param group The type of grouping referenced
         * @return The {@link Builder} with a new grouping entry added
         */
        public Builder groupBy(String name, SingleGroupSource group) {
            groups.put(name, group);
            return this;
        }

        public GroupConfig build() {
            return new GroupConfig(groups);
        }
    }
}
