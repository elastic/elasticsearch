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
package org.elasticsearch.client.enrich;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public final class NamedPolicy {

    static final ParseField NAME_FIELD = new ParseField("name");
    static final ParseField QUERY_FIELD = new ParseField("query");
    static final ParseField INDICES_FIELD = new ParseField("indices");
    static final ParseField MATCH_FIELD_FIELD = new ParseField("match_field");
    static final ParseField ENRICH_FIELDS_FIELD = new ParseField("enrich_fields");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<NamedPolicy, String> PARSER = new ConstructingObjectParser<>(
        "policy",
        true,
        (args, policyType) -> new NamedPolicy(
            policyType,
            (String) args[0],
            (BytesReference) args[1],
            (List<String>) args[2],
            (String) args[3],
            (List<String>) args[4]
        )
    );

    static {
        declareParserOptions(PARSER);
    }

    private static void declareParserOptions(ConstructingObjectParser<?, ?> parser) {
        parser.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentBuilder builder = XContentBuilder.builder(p.contentType().xContent());
            builder.copyCurrentStructure(p);
            return BytesReference.bytes(builder);
        }, QUERY_FIELD);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), MATCH_FIELD_FIELD);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), ENRICH_FIELDS_FIELD);
    }

    public static NamedPolicy fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        String policyType = parser.currentName();
        NamedPolicy policy = PARSER.parse(parser, policyType);
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        return policy;
    }

    private final String type;
    private final String name;
    private final BytesReference query;
    private final List<String> indices;
    private final String matchField;
    private final List<String> enrichFields;

    NamedPolicy(String type, String name, BytesReference query, List<String> indices, String matchField, List<String> enrichFields) {
        this.type = type;
        this.name = name;
        this.query = query;
        this.indices = indices;
        this.matchField = matchField;
        this.enrichFields = enrichFields;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public BytesReference getQuery() {
        return query;
    }

    public List<String> getIndices() {
        return indices;
    }

    public String getMatchField() {
        return matchField;
    }

    public List<String> getEnrichFields() {
        return enrichFields;
    }
}
