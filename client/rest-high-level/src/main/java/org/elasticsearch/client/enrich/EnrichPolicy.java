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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public final class EnrichPolicy {

    static final ParseField TYPE_FIELD = new ParseField("type");
    static final ParseField QUERY_FIELD = new ParseField("query");
    static final ParseField INDICES_FIELD = new ParseField("indices");
    static final ParseField ENRICH_KEY_FIELD = new ParseField("enrich_key");
    static final ParseField ENRICH_VALUES_FIELD = new ParseField("enrich_values");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EnrichPolicy, Void> PARSER = new ConstructingObjectParser<>("policy",
        args -> new EnrichPolicy(
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
        parser.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentBuilder builder = XContentBuilder.builder(p.contentType().xContent());
            builder.copyCurrentStructure(p);
            return BytesArray.bytes(builder);
        }, QUERY_FIELD);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), ENRICH_KEY_FIELD);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), ENRICH_VALUES_FIELD);
    }

    public static EnrichPolicy fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String type;
    private final BytesReference query;
    private final List<String> indices;
    private final String enrichKey;
    private final List<String> enrichValues;

    EnrichPolicy(String type, BytesReference query, List<String> indices, String enrichKey, List<String> enrichValues) {
        this.type = type;
        this.query = query;
        this.indices = indices;
        this.enrichKey = enrichKey;
        this.enrichValues = enrichValues;
    }

    public String getType() {
        return type;
    }

    public BytesReference getQuery() {
        return query;
    }

    public List<String> getIndices() {
        return indices;
    }

    public String getEnrichKey() {
        return enrichKey;
    }

    public List<String> getEnrichValues() {
        return enrichValues;
    }
}
