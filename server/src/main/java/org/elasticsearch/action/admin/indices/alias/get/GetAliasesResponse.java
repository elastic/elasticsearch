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

package org.elasticsearch.action.admin.indices.alias.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetAliasesResponse extends ActionResponse implements StatusToXContentObject {

    private ImmutableOpenMap<String, List<AliasMetaData>> aliases = ImmutableOpenMap.of();
    private RestStatus status = RestStatus.OK;
    private String errorMessage;

    public GetAliasesResponse(ImmutableOpenMap<String, List<AliasMetaData>> aliases, RestStatus status, String errorMessage) {
        this.aliases = aliases;
        this.status = status == null ? RestStatus.OK : status;
        this.errorMessage = errorMessage;
    }

    public GetAliasesResponse(ImmutableOpenMap<String, List<AliasMetaData>> aliases) {
        this(aliases, RestStatus.OK, null);
    }

    GetAliasesResponse() {
    }

    @Override
    public RestStatus status() {
        return status;
    }

    public ImmutableOpenMap<String, List<AliasMetaData>> getAliases() {
        return aliases;
    }

    /**
     * Returns the error message if the request has not been executed successfully or null otherwise.
     * 
     * <p>
     * Used only by the high-level REST client.
     * 
     * @return The error message.
     */
    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            List<AliasMetaData> value = new ArrayList<>(valueSize);
            for (int j = 0; j < valueSize; j++) {
                value.add(new AliasMetaData(in));
            }
            aliasesBuilder.put(key, Collections.unmodifiableList(value));
        }
        aliases = aliasesBuilder.build();
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            // if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            status = RestStatus.readFrom(in);
            if (in.readBoolean()) {
                errorMessage = in.readString();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(aliases.size());
        for (ObjectObjectCursor<String, List<AliasMetaData>> entry : aliases) {
            out.writeString(entry.key);
            out.writeVInt(entry.value.size());
            for (AliasMetaData aliasMetaData : entry.value) {
                aliasMetaData.writeTo(out);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            // if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            RestStatus.writeTo(out, status);
            out.writeOptionalString(errorMessage);
        }
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
        return Objects.equals(fromListAliasesToSet(aliases), fromListAliasesToSet(that.aliases))
                && Objects.equals(status, that.status)
                && Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromListAliasesToSet(aliases), status, errorMessage);
    }

    private static ImmutableOpenMap<String, Set<AliasMetaData>> fromListAliasesToSet(ImmutableOpenMap<String, List<AliasMetaData>> list) {
        ImmutableOpenMap.Builder<String, Set<AliasMetaData>> builder = ImmutableOpenMap.builder();
        list.forEach(e -> builder.put(e.key, new HashSet<>(e.value)));
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        final boolean namesProvided = (params.param("name") != null);
        final ImmutableOpenMap<String, List<AliasMetaData>> aliasMap = this.aliases;

        final Set<String> indicesToDisplay = new HashSet<>();

        if (namesProvided) {
            for (final ObjectObjectCursor<String, List<AliasMetaData>> cursor : aliasMap) {
                if (cursor.value != null && false == cursor.value.isEmpty()) {
                    indicesToDisplay.add(cursor.key);
                }
            }
        }
        
        builder.startObject();
        {
            if (status != null && RestStatus.OK != status) {
                builder.field("error", errorMessage);
                builder.field("status", status.getStatus());
            }

            for (final ObjectObjectCursor<String, List<AliasMetaData>> entry : aliases) {
                if (false == namesProvided || indicesToDisplay.contains(entry.key)) {
                    builder.startObject(entry.key);
                    {
                        builder.startObject("aliases");
                        {
                            for (final AliasMetaData alias : entry.value) {
                                AliasMetaData.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
                            }
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
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
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesBuilder = ImmutableOpenMap.builder();

        String currentFieldName;
        Token token;
        String exceptionMessage = null;
        RestStatus status = RestStatus.OK;

        while (parser.nextToken() != Token.END_OBJECT) {
            if (parser.currentToken() == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();

                if ("status".equals(currentFieldName)) {
                    if ((token = parser.nextToken()) != Token.FIELD_NAME) {
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser::getTokenLocation);
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
                        List<AliasMetaData> parseInside = parseAliases(parser);
                        aliasesBuilder.put(indexName, parseInside);
                    }
                }
            }
        }
        return new GetAliasesResponse(aliasesBuilder.build(), status, exceptionMessage);
    }

    private static List<AliasMetaData> parseAliases(XContentParser parser) throws IOException {
        List<AliasMetaData> aliases = new ArrayList<>();
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
