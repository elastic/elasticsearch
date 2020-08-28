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

package org.elasticsearch.client.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response when invalidating one or multiple OAuth2 access tokens and refresh tokens. Returns
 * information concerning how many tokens were invalidated, how many of the tokens that
 * were attempted to be invalidated were already invalid, and if there were any errors
 * encountered.
 */
public final class InvalidateTokenResponse {

    public static final ParseField INVALIDATED_TOKENS = new ParseField("invalidated_tokens");
    public static final ParseField PREVIOUSLY_INVALIDATED_TOKENS = new ParseField("previously_invalidated_tokens");
    public static final ParseField ERROR_COUNT = new ParseField("error_count");
    public static final ParseField ERRORS = new ParseField("error_details");

    private final int invalidatedTokens;
    private final int previouslyInvalidatedTokens;
    private List<ElasticsearchException> errors;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InvalidateTokenResponse, Void> PARSER = new ConstructingObjectParser<>(
        "tokens_invalidation_result", true,
        // we parse but do not use the count of errors as we implicitly have this in the size of the Exceptions list
        args -> new InvalidateTokenResponse((int) args[0], (int) args[1], (List<ElasticsearchException>) args[3]));

    static {
        PARSER.declareInt(constructorArg(), INVALIDATED_TOKENS);
        PARSER.declareInt(constructorArg(), PREVIOUSLY_INVALIDATED_TOKENS);
        PARSER.declareInt(constructorArg(), ERROR_COUNT);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), ERRORS);

    }

    public InvalidateTokenResponse(int invalidatedTokens, int previouslyInvalidatedTokens, @Nullable List<ElasticsearchException> errors) {
        this.invalidatedTokens = invalidatedTokens;
        this.previouslyInvalidatedTokens = previouslyInvalidatedTokens;
        if (null == errors) {
            this.errors = Collections.emptyList();
        } else {
            this.errors = Collections.unmodifiableList(errors);
        }
    }

    public int getInvalidatedTokens() {
        return invalidatedTokens;
    }

    public int getPreviouslyInvalidatedTokens() {
        return previouslyInvalidatedTokens;
    }

    public List<ElasticsearchException> getErrors() {
        return errors;
    }

    public int getErrorsCount() {
        return errors == null ? 0 : errors.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvalidateTokenResponse that = (InvalidateTokenResponse) o;
        return invalidatedTokens == that.invalidatedTokens &&
            previouslyInvalidatedTokens == that.previouslyInvalidatedTokens &&
            Objects.equals(errors, that.errors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(invalidatedTokens, previouslyInvalidatedTokens, errors);
    }

    public static InvalidateTokenResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        return PARSER.parse(parser, null);
    }
}
