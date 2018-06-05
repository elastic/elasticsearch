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

package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetAliasesResponse extends ActionResponse implements StatusToXContentObject {

    private final RestStatus status;
    private final String errorMessage;
    private final Map<String, Set<AliasMetaData>> aliases;

    public GetAliasesResponse(RestStatus status, String errorMessage, Map<String, Set<AliasMetaData>> aliases) {
        this.status = status;
        this.errorMessage = errorMessage;
        this.aliases = aliases;
    }

    @Override
    public RestStatus status() {
        return status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Map<String, Set<AliasMetaData>> getAliases() {
        return aliases;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetAliasesResponse that = (GetAliasesResponse) o;
        return status == that.status &&
                Objects.equals(errorMessage, that.errorMessage) &&
                Objects.equals(aliases, that.aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, errorMessage, aliases);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (status != RestStatus.OK) {
                builder.field("error", errorMessage);
                builder.field("status", status.getStatus());
            }

            for (Map.Entry<String, Set<AliasMetaData>> entry : aliases.entrySet()) {
                builder.startObject(entry.getKey());
                {
                    builder.startObject("aliases");
                    {
                        for (final AliasMetaData alias : entry.getValue()) {
                            AliasMetaData.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    public static GetAliasesResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        Map<String, Set<AliasMetaData>> aliases = new HashMap<>();

        String currentFieldName;
        Token token;
        String exceptionMessage = null;
        RestStatus status = RestStatus.OK;

        while (parser.nextToken() != Token.END_OBJECT) {
            if (parser.currentToken() == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();

                if ("status".equals(currentFieldName)) {
                    if ((token = parser.nextToken()) != Token.FIELD_NAME) {
                        ensureExpectedToken(Token.VALUE_NUMBER, token, parser::getTokenLocation);
                        status = RestStatus.fromCode(parser.intValue());
                    }
                } else if ("error".equals(currentFieldName)) {
                    if ((token = parser.nextToken()) != Token.FIELD_NAME) {
                        if (token == Token.VALUE_STRING) {
                            exceptionMessage = parser.text();
                        } else if (token == Token.START_OBJECT) {
                            parser.nextToken();
                            exceptionMessage = ElasticsearchException.innerFromXContent(parser, true).getMessage();
                        }
                    }
                } else {
                    String indexName = parser.currentName();
                    if (parser.nextToken() == Token.START_OBJECT) {
                        Set<AliasMetaData> parseInside = parseAliases(parser);
                        aliases.put(indexName, parseInside);
                    }
                }
            }
        }
        return new GetAliasesResponse(status, exceptionMessage, aliases);
    }

    private static Set<AliasMetaData> parseAliases(XContentParser parser) throws IOException {
        Set<AliasMetaData> aliases = new HashSet<>();
        Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                if ("aliases".equals(currentFieldName)) {
                    while (parser.nextToken() != Token.END_OBJECT) {
                        AliasMetaData fromXContent = AliasMetaData.Builder.fromXContent(parser);
                        aliases.add(fromXContent);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return aliases;
    }
}
