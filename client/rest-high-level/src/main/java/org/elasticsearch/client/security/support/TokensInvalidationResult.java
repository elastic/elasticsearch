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

package org.elasticsearch.client.security.support;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TokensInvalidationResult implements ToXContentObject {

    public static final ParseField INVALIDATED_TOKENS = new ParseField("invalidated_tokens");
    public static final ParseField PREVIOUSLY_INVALIDATED_TOKENS = new ParseField("previously_invalidated_tokens");
    public static final ParseField ERROR_SIZE = new ParseField("error_size");
    public static final ParseField ERRORS = new ParseField("error_details");

    private final int invalidatedTokens;
    private final int previouslyInvalidatedTokens;
    private List<ElasticsearchException> errors;

    public TokensInvalidationResult(int invalidatedTokens, int previouslyInvalidatedTokens, @Nullable List<ElasticsearchException> errors) {
        this.invalidatedTokens = invalidatedTokens;
        this.previouslyInvalidatedTokens = previouslyInvalidatedTokens;
        if (null == errors) {
            this.errors = Collections.emptyList();
        } else {
            this.errors = errors;
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

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TokensInvalidationResult, Void> PARSER = new ConstructingObjectParser<>(
        "tokens_invalidation_result", true,
        // we parse but do not use the size of errors as we implicitly have this in the size of the Exceptions list
        args -> new TokensInvalidationResult((int) args[0], (int) args[1], (List<ElasticsearchException>) args[3]));

    static {
        PARSER.declareInt(constructorArg(), INVALIDATED_TOKENS);
        PARSER.declareInt(constructorArg(), PREVIOUSLY_INVALIDATED_TOKENS);
        PARSER.declareInt(constructorArg(), ERROR_SIZE);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), ERRORS);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokensInvalidationResult that = (TokensInvalidationResult) o;
        return invalidatedTokens == that.invalidatedTokens &&
            previouslyInvalidatedTokens == that.previouslyInvalidatedTokens &&
            Objects.equals(errors, that.errors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(invalidatedTokens, previouslyInvalidatedTokens, errors);
    }

    public static TokensInvalidationResult fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("invalidated_tokens", invalidatedTokens)
            .field("previously_invalidated_tokens", previouslyInvalidatedTokens)
            .field("error_size", errors.size());
        if (errors.isEmpty() == false) {
            builder.field("error_details");
            builder.startArray();
            for (ElasticsearchException e : errors) {
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, e);
                builder.endObject();
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    public String toString() {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
