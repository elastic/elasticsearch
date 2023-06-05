/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class QueryRuleset implements Writeable, ToXContent {

    private final Logger logger = LogManager.getLogger(QueryRuleset.class);

    private final String id;
    private final List<QueryRule> rules;

    public QueryRuleset(String id, List<QueryRule> rules) {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("id cannot be null or empty");
        }
        this.id = id;

        Objects.requireNonNull(rules, "rules cannot be null");
        this.rules = rules;
    }

    public QueryRuleset(StreamInput in) throws IOException {
        this.id = in.readString();
        int numRules = in.readVInt();
        List<QueryRule> rules = new ArrayList<>();
        for (int i = 0; i < numRules; i++) {
            rules.add(new QueryRule(in));
        }
        this.rules = rules;
    }

    private static final ConstructingObjectParser<QueryRuleset, String> PARSER = new ConstructingObjectParser<>(
        "query_ruleset",
        false,
        (params, resourceName) -> {
            final String id = (String) params[0];
            // Check that id matches the resource name. We don't want it to be updatable
            if (id.equals(resourceName) == false) {
                throw new IllegalArgumentException(
                    "Query ruleset identifier [" + id + "] does not match the resource name: [" + resourceName + "]"
                );
            }
            @SuppressWarnings("unchecked")
            final List<QueryRule> rules = (List<QueryRule>) params[1];

            return new QueryRuleset(resourceName, rules);
        }
    );

    public static final ParseField ID_FIELD = new ParseField("ruleset_id");
    public static final ParseField RULES_FIELD = new ParseField("rules");

    static {
        PARSER.declareString(constructorArg(), ID_FIELD);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> QueryRule.fromXContent(p), RULES_FIELD);
    }

    /**
     * Parses a {@link QueryRuleset} from its {@param xContentType} representation in bytes.
     *
     * @param resourceName The name of the resource (must match the {@link QueryRuleset} id).
     * @param source The bytes that represents the {@link QueryRuleset}.
     * @param xContentType The format of the representation.
     *
     * @return The parsed {@link QueryRuleset}.
     */
    public static QueryRuleset fromXContentBytes(String resourceName, BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return QueryRuleset.fromXContent(resourceName, parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    /**
     * Parses a {@link QueryRuleset} through the provided {@param parser}.
     *
     * @param resourceName The name of the resource (must match the {@link QueryRuleset} name).
     * @param parser The {@link XContentType} parser.
     *
     * @return The parsed {@link QueryRuleset}.
     */
    public static QueryRuleset fromXContent(String resourceName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, resourceName);
    }

    /**
     * Converts the {@link QueryRuleset} to XContent.
     *
     * @return The {@link XContentBuilder} containing the serialized {@link QueryRuleset}.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO I don't understand why serialization tests fail if we have startObject/endObject here.
        builder.startObject();
        {
            builder.field(ID_FIELD.getPreferredName(), id);
            builder.startArray(RULES_FIELD.getPreferredName());
            for (QueryRule rule : rules) {
                rule.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVInt(rules.size());
        for (QueryRule rule : rules) {
            rule.writeTo(out);
        }
    }

    public String id() {
        return id;
    }

    public List<QueryRule> rules() {
        return rules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRuleset that = (QueryRuleset) o;
        return id.equals(that.id) && Objects.equals(rules, that.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rules);
    }
}
