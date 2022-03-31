/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response obtained from the get aliases API.
 * The format is pretty horrible as it holds aliases, but at the same time errors can come back through the status and error fields.
 * Such errors are mostly 404 - NOT FOUND for aliases that were specified but not found. In such case the client won't throw exception
 * so it allows to retrieve the returned aliases, while at the same time checking if errors were returned.
 * There's also the case where an exception is returned, like for instance an {@link org.elasticsearch.index.IndexNotFoundException}.
 * We would usually throw such exception, but we configure the client to not throw for 404 to support the case above, hence we also not
 * throw in case an index is not found, although it is a hard error that doesn't come back with aliases.
 */
public class GetAliasesResponse implements StatusToXContentObject {

    private final RestStatus status;
    private final String error;
    private final ElasticsearchException exception;

    private final Map<String, Set<AliasMetadata>> aliases;

    GetAliasesResponse(RestStatus status, String error, Map<String, Set<AliasMetadata>> aliases) {
        this.status = status;
        this.error = error;
        this.aliases = aliases;
        this.exception = null;
    }

    private GetAliasesResponse(RestStatus status, ElasticsearchException exception) {
        this.status = status;
        this.error = null;
        this.aliases = Collections.emptyMap();
        this.exception = exception;
    }

    @Override
    public RestStatus status() {
        return status;
    }

    /**
     * Return the possibly returned error, null otherwise
     */
    public String getError() {
        return error;
    }

    /**
     * Return the exception that may have been returned
     */
    public ElasticsearchException getException() {
        return exception;
    }

    /**
     * Return the requested aliases
     */
    public Map<String, Set<AliasMetadata>> getAliases() {
        return aliases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (status != RestStatus.OK) {
                builder.field("error", error);
                builder.field("status", status.getStatus());
            }

            for (Map.Entry<String, Set<AliasMetadata>> entry : aliases.entrySet()) {
                builder.startObject(entry.getKey());
                {
                    builder.startObject("aliases");
                    {
                        for (final AliasMetadata alias : entry.getValue()) {
                            AliasMetadata.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
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

    /**
     * Parse the get aliases response
     */
    public static GetAliasesResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser);
        Map<String, Set<AliasMetadata>> aliases = new HashMap<>();

        String currentFieldName;
        Token token;
        String error = null;
        ElasticsearchException exception = null;
        RestStatus status = RestStatus.OK;

        while (parser.nextToken() != Token.END_OBJECT) {
            if (parser.currentToken() == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();

                if ("status".equals(currentFieldName)) {
                    if ((token = parser.nextToken()) != Token.FIELD_NAME) {
                        ensureExpectedToken(Token.VALUE_NUMBER, token, parser);
                        status = RestStatus.fromCode(parser.intValue());
                    }
                } else if ("error".equals(currentFieldName)) {
                    token = parser.nextToken();
                    if (token == Token.VALUE_STRING) {
                        error = parser.text();
                    } else if (token == Token.START_OBJECT) {
                        parser.nextToken();
                        exception = ElasticsearchException.innerFromXContent(parser, true);
                    } else if (token == Token.START_ARRAY) {
                        parser.skipChildren();
                    }
                } else {
                    String indexName = parser.currentName();
                    if (parser.nextToken() == Token.START_OBJECT) {
                        Set<AliasMetadata> parseInside = parseAliases(parser);
                        aliases.put(indexName, parseInside);
                    }
                }
            }
        }
        if (exception != null) {
            assert error == null;
            assert aliases.isEmpty();
            return new GetAliasesResponse(status, exception);
        }
        return new GetAliasesResponse(status, error, aliases);
    }

    private static Set<AliasMetadata> parseAliases(XContentParser parser) throws IOException {
        Set<AliasMetadata> aliases = new HashSet<>();
        Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                if ("aliases".equals(currentFieldName)) {
                    while (parser.nextToken() != Token.END_OBJECT) {
                        AliasMetadata fromXContent = AliasMetadata.Builder.fromXContent(parser);
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
