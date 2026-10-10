/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * A lookup declared by a templated DLS role query: a request for externally resolved data that the template can reference.
 * <p>
 * Lookups are declared in a {@code lookups} object that is a sibling of the {@code template} object in the role query:
 * <pre>{@code
 * {
 *   "template": { "source": "{\"terms\":{\"ml_job_id\":{{#toJson}}_lookup.ml_jobs{{/toJson}}}}" },
 *   "lookups": {
 *     "ml_jobs": { "type": "kibana_ml_job_ids", "params": { "spaces": ["marketing"] } }
 *   }
 * }
 * }</pre>
 * Each entry binds a {@link #name() name}, which the Mustache template reads under {@code _lookup.<name>}, to a resolver
 * {@link #type() type} registered by a {@link org.elasticsearch.xpack.core.security.SecurityExtension} and the
 * {@link #params() params} handed to that resolver. Resolution happens once per request, on the coordinating node, before the
 * authorized action is dispatched; see {@link DlsLookupResolver}.
 * <p>
 * The lookup is identified by its {@link #key() key}, a canonical rendering of {@code type} and {@code params}. Two queries
 * declaring the same type and params share a single resolution regardless of the name each binds it to.
 */
public record DlsLookup(String name, String type, Map<String, Object> params) {

    public static final ParseField LOOKUPS_FIELD = new ParseField("lookups");
    static final ParseField TYPE_FIELD = new ParseField("type");
    static final ParseField PARAMS_FIELD = new ParseField("params");

    private static final Pattern VALID_NAME = Pattern.compile("^[A-Za-z0-9_]+$");

    public DlsLookup {
        Objects.requireNonNull(name, "lookup name must not be null");
        Objects.requireNonNull(type, "lookup type must not be null");
        if (VALID_NAME.matcher(name).matches() == false) {
            throw new IllegalArgumentException(
                "invalid DLS lookup name [" + name + "]: names may only contain letters, digits and underscores"
            );
        }
        if (Strings.hasText(type) == false) {
            throw new IllegalArgumentException("DLS lookup [" + name + "] must declare a non-empty type");
        }
        params = params == null ? Map.of() : Map.copyOf(params);
    }

    /**
     * A canonical identifier for this lookup derived from {@link #type()} and {@link #params()} only. Map keys are sorted
     * recursively so that semantically equal params produce the same key irrespective of declaration order. The name is
     * deliberately excluded: it is a template-local binding, not part of the lookup's identity.
     */
    public String key() {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(PARAMS_FIELD.getPreferredName());
            builder.value(canonicalize(params));
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Object canonicalize(Object value) {
        if (value instanceof Map<?, ?> map) {
            final TreeMap<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                sorted.put(String.valueOf(entry.getKey()), canonicalize(entry.getValue()));
            }
            return sorted;
        } else if (value instanceof Iterable<?> iterable) {
            final List<Object> list = new ArrayList<>();
            for (Object element : iterable) {
                list.add(canonicalize(element));
            }
            return list;
        }
        return value;
    }

    /**
     * Returns the lookups declared by the given role query, or an empty list if the query is not a template or declares
     * none. Non-template queries cannot declare lookups, so their content is not inspected beyond the leading field.
     *
     * @throws ElasticsearchParseException if the query is malformed or a declared lookup is invalid
     */
    public static List<DlsLookup> extractFromRoleQuery(BytesReference query) {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, query.streamInput())) {
            if (DLSRoleQueryValidator.isTemplateQuery(parser) == false) {
                return List.of();
            }
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("expected [template] to be an object but found [{}]", parser.currentToken());
            }
            parser.skipChildren();
            return parseSiblingsOfTemplate(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse role query", e);
        }
    }

    /**
     * Consumes the fields that follow the {@code template} object up to the closing brace of the role query, returning any
     * declared lookups. Fields other than {@code lookups} are skipped: historically the evaluator ignored trailing siblings, and
     * rejecting them at evaluation time would fail requests for roles that were previously accepted.
     * <p>
     * The parser must be positioned on the last token of the {@code template} object.
     */
    static List<DlsLookup> parseSiblingsOfTemplate(XContentParser parser) throws IOException {
        List<DlsLookup> lookups = List.of();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ElasticsearchParseException("unexpected token [{}] in role query", token);
            }
            final String fieldName = parser.currentName();
            parser.nextToken();
            if (LOOKUPS_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                // the parser rejects duplicate field names, so this branch is taken at most once
                lookups = parseLookups(parser);
            } else {
                parser.skipChildren();
            }
        }
        return lookups;
    }

    /**
     * Parses the {@code lookups} object. The parser must be positioned on its {@link XContentParser.Token#START_OBJECT}.
     */
    static List<DlsLookup> parseLookups(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "expected [{}] to be an object but found [{}]",
                LOOKUPS_FIELD.getPreferredName(),
                parser.currentToken()
            );
        }
        final List<DlsLookup> lookups = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ElasticsearchParseException("unexpected token [{}] in [{}]", token, LOOKUPS_FIELD.getPreferredName());
            }
            final String name = parser.currentName();
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("expected lookup [{}] to be an object but found [{}]", name, parser.currentToken());
            }
            lookups.add(parseLookup(name, parser));
        }
        // names are unique because the parser rejects duplicate field names
        return List.copyOf(lookups);
    }

    private static DlsLookup parseLookup(String name, XContentParser parser) throws IOException {
        String type = null;
        Map<String, Object> params = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ElasticsearchParseException("unexpected token [{}] in lookup [{}]", token, name);
            }
            final String fieldName = parser.currentName();
            token = parser.nextToken();
            if (TYPE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                if (token != XContentParser.Token.VALUE_STRING) {
                    throw new ElasticsearchParseException("expected [type] of lookup [{}] to be a string but found [{}]", name, token);
                }
                type = parser.text();
            } else if (PARAMS_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException("expected [params] of lookup [{}] to be an object but found [{}]", name, token);
                }
                params = parser.map();
            } else {
                throw new ElasticsearchParseException("unknown field [{}] in lookup [{}]", fieldName, name);
            }
        }
        if (type == null) {
            throw new ElasticsearchParseException("lookup [{}] is missing required field [type]", name);
        }
        try {
            return new DlsLookup(name, type, params);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchParseException(e.getMessage(), e);
        }
    }
}
