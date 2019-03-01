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

package org.elasticsearch.client.dataframe.transforms.pivot;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GroupConfig implements ToXContentObject {

    private final Map<String, SingleGroupSource> groups;

    public static GroupConfig fromXContent(final XContentParser parser) throws IOException {
        LinkedHashMap<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // be parsing friendly, whether the token needs to be advanced or not (similar to what ObjectParser does)
        XContentParser.Token token;
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            token = parser.currentToken();
        } else {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Failed to parse object: Expected START_OBJECT but was: " + token);
            }
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            String destinationFieldName = parser.currentName();
            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            SingleGroupSource.Type groupType = SingleGroupSource.Type.valueOf(parser.currentName().toUpperCase(Locale.ROOT));

            token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
            SingleGroupSource groupSource;
            switch (groupType) {
                case TERMS:
                    groupSource = TermsGroupSource.fromXContent(parser);
                    break;
                case HISTOGRAM:
                    groupSource = HistogramGroupSource.fromXContent(parser);
                    break;
                case DATE_HISTOGRAM:
                    groupSource = DateHistogramGroupSource.fromXContent(parser);
                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "invalid grouping type: " + groupType);
            }

            parser.nextToken();

            groups.put(destinationFieldName, groupSource);
        }
        return new GroupConfig(groups);
    }

    public GroupConfig(Map<String, SingleGroupSource> groups) {
        this.groups = groups;
    }

    public Map <String, SingleGroupSource> getGroups() {
        return groups;
    }

    public boolean isValid() {
        return this.groups != null;
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
}
