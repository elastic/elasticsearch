/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class QueryRuleset implements Writeable, ToXContentObject {

    private final String id;
    private final List<QueryRule> rules;

    /**
     * A {@link QueryRuleset} is composed of a unique identifier and a collection of
     * one or more associated {@link QueryRule}s.
     *
     * @param id A Unique identifier representing the query ruleset.
     * @param rules A collection of one or more {@link QueryRule}s.
     */
    public QueryRuleset(String id, List<QueryRule> rules) {
        this.id = id;
        this.rules = rules;
    }

    public QueryRuleset(StreamInput in) throws IOException {
        this.id = in.readString();
        this.rules = in.readList(QueryRule::new);
    }

    private static final ConstructingObjectParser<QueryRuleset, String> PARSER = new ConstructingObjectParser<>(
        "query_ruleset",
        false,
        (params, resourceName) -> {
            final String id = (String) params[0];
            // Check that id matches the resource name. We don't want it to be updatable
            if (id != null && id.equals(resourceName) == false) {
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
        PARSER.declareString(optionalConstructorArg(), ID_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> QueryRule.fromXContent(p), RULES_FIELD);
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
        builder.startObject();
        {
            builder.field(ID_FIELD.getPreferredName(), id);
            builder.xContentList(RULES_FIELD.getPreferredName(), rules);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeList(rules);
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
