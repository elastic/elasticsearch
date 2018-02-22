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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetAliasesResponse extends ActionResponse implements StatusToXContentObject {

    private ImmutableOpenMap<String, List<AliasMetaData>> aliases = ImmutableOpenMap.of();
    private RestStatus status;

    public GetAliasesResponse(ImmutableOpenMap<String, List<AliasMetaData>> aliases) {
        this.aliases = aliases;
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
        return Objects.equals(fromListAliasesToSet(aliases), fromListAliasesToSet(that.aliases));
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromListAliasesToSet(aliases));
    }

    private ImmutableOpenMap<String, Set<AliasMetaData>> fromListAliasesToSet(ImmutableOpenMap<String, List<AliasMetaData>> list) {
        ImmutableOpenMap.Builder<String, Set<AliasMetaData>> builder = ImmutableOpenMap.builder();
        list.forEach(e -> builder.put(e.key, new HashSet<>(e.value)));
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        final boolean namesProvided = (params.param("name") != null);
        String[] aliasesNames = Strings.EMPTY_ARRAY;
        final ImmutableOpenMap<String, List<AliasMetaData>> aliasMap = this.aliases;

        if (params.param("name") != null) {
            String[] result = Strings.splitStringByCommaToArray(params.param("name"));
            if (false == Strings.isAllOrWildcard(result)) {
                aliasesNames = result;
            }
        }

        final Set<String> aliasNames = new HashSet<>();
        final Set<String> indicesToDisplay = new HashSet<>();
        for (final ObjectObjectCursor<String, List<AliasMetaData>> cursor : aliasMap) {
            for (final AliasMetaData aliasMetaData : cursor.value) {
                aliasNames.add(aliasMetaData.alias());
                if (namesProvided) {
                    indicesToDisplay.add(cursor.key);
                }
            }
        }

        // first remove requested aliases that are exact matches
        final SortedSet<String> difference = Sets.sortedDifference(Arrays.stream(aliasesNames).collect(Collectors.toSet()), aliasNames);

        // now remove requested aliases that contain wildcards that are simple matches
        final List<String> matches = new ArrayList<>();
        outer:
        for (final String pattern : difference) {
            if (pattern.contains("*")) {
                for (final String aliasName : aliasNames) {
                    if (Regex.simpleMatch(pattern, aliasName)) {
                        matches.add(pattern);
                        continue outer;
                    }
                }
            }
        }
        difference.removeAll(matches);

        builder.startObject();
        {
            if (difference.isEmpty()) {
                status = RestStatus.OK;
            } else {
                status = RestStatus.NOT_FOUND;
                final String message;
                if (difference.size() == 1) {
                    message = String.format(Locale.ROOT, "alias [%s] missing", toNamesString(difference.iterator().next()));
                } else {
                    message = String.format(Locale.ROOT, "aliases [%s] missing", toNamesString(difference.toArray(new String[0])));
                }
                builder.field("error", message);
                builder.field("status", status.getStatus());
            }

            for (final ObjectObjectCursor<String, List<AliasMetaData>> entry : aliases) {
                if (namesProvided == false || (namesProvided && indicesToDisplay.contains(entry.key))) {
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
    
    private static String toNamesString(final String... names) {
        if (names == null || names.length == 0) {
            return "";
        } else if (names.length == 1) {
            return names[0];
        } else {
            return Arrays.stream(names).collect(Collectors.joining(","));
        }
    }

    public static GetAliasesResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesBuilder = ImmutableOpenMap.builder();

        while ((parser.nextToken()) != Token.END_OBJECT) {
            if (parser.currentToken() == Token.FIELD_NAME) {
                String indexName = parser.currentName();
                if (parser.nextToken() == Token.START_OBJECT) {
                    List<AliasMetaData> parseInside = parseAliases(parser);
                    aliasesBuilder.put(indexName, parseInside);
                }
            }
        }
        GetAliasesResponse getAliasesResponse = new GetAliasesResponse(aliasesBuilder.build());
        return getAliasesResponse;
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
            }
        }
        return aliases;
    }

}
