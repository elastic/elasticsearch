/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Collections;

/**
 * This class helps in evaluating the query field if it is template,
 * validating the query and checking if the query type is allowed to be used in DLS role query.
 */
public final class DLSRoleQueryValidator {

    private DLSRoleQueryValidator() {}

    /**
     * Validates the query field in the {@link RoleDescriptor.IndicesPrivileges} only if it is not a template query.<br>
     * It parses the query and builds the {@link QueryBuilder}, also checks if the query type is supported in DLS role query.
     *
     * @param indicesPrivileges {@link RoleDescriptor.IndicesPrivileges}
     * @param xContentRegistry  {@link NamedXContentRegistry} for finding named queries
     */
    public static void validateQueryField(RoleDescriptor.IndicesPrivileges[] indicesPrivileges, NamedXContentRegistry xContentRegistry) {
        if (indicesPrivileges != null) {
            for (int i = 0; i < indicesPrivileges.length; i++) {
                BytesReference query = indicesPrivileges[i].getQuery();
                try {
                    if (query != null) {
                        if (isTemplateQuery(query, xContentRegistry)) {
                            // skip template query, this requires runtime information like 'User' information.
                            continue;
                        }

                        evaluateAndVerifyRoleQuery(query.utf8ToString(), xContentRegistry);
                    }
                } catch (ParsingException | IllegalArgumentException | IOException e) {
                    throw new ElasticsearchParseException(
                        "failed to parse field 'query' for indices ["
                            + Strings.arrayToCommaDelimitedString(indicesPrivileges[i].getIndices())
                            + "] at index privilege ["
                            + i
                            + "] of role descriptor",
                        e
                    );
                }
            }
        }
    }

    private static boolean isTemplateQuery(BytesReference query, NamedXContentRegistry xContentRegistry) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(parserConfig(xContentRegistry), query.utf8ToString())) {
            return isTemplateQuery(parser);
        }
    }

    private static boolean isTemplateQuery(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "expected [" + XContentParser.Token.START_OBJECT + "] but " + "found [" + token + "] instead"
            );
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "expected ["
                    + XContentParser.Token.FIELD_NAME
                    + "] with "
                    + "value a query name or 'template' but found ["
                    + token
                    + "] instead"
            );
        }
        String fieldName = parser.currentName();
        return "template".equals(fieldName);
    }

    public static boolean hasStoredScript(BytesReference query, NamedXContentRegistry xContentRegistry) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(parserConfig(xContentRegistry), query.utf8ToString())) {
            if (false == isTemplateQuery(parser)) {
                return false;
            }
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "expected [" + XContentParser.Token.START_OBJECT + "] but found [" + parser.currentToken() + "] instead"
                );
            }
            return ScriptType.STORED == Script.parse(parser).getType();
        }
    }

    /**
     * Evaluates the query if it is a template and then validates the query by parsing
     * and building the {@link QueryBuilder}. It also checks if the query type is
     * supported in DLS role query.
     *
     * @param query            {@link BytesReference} query field from the role
     * @param scriptService    {@link ScriptService} used for evaluation of a template query
     * @param xContentRegistry {@link NamedXContentRegistry} for finding named queries
     * @param user             {@link User} used when evaluation a template query
     * @return {@link QueryBuilder} if the query is valid and allowed, in case {@link RoleDescriptor.IndicesPrivileges}
     * * does not have a query field then it returns {@code null}.
     */
    @Nullable
    public static QueryBuilder evaluateAndVerifyRoleQuery(
        BytesReference query,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        User user
    ) {
        if (query != null) {
            String templateResult = SecurityQueryTemplateEvaluator.evaluateTemplate(query.utf8ToString(), scriptService, user);
            try {
                return evaluateAndVerifyRoleQuery(templateResult, xContentRegistry);
            } catch (ElasticsearchParseException | ParsingException | XContentParseException | IOException e) {
                throw new ElasticsearchParseException("failed to parse field 'query' from the role descriptor", e);
            }
        }
        return null;
    }

    @Nullable
    public static QueryBuilder evaluateAndVerifyRoleQuery(String query, NamedXContentRegistry xContentRegistry) throws IOException {
        if (query != null) {
            NamedXContentRegistry registryWrapper = new NamedXContentRegistry(Collections.emptyList()) {
                @Override
                public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
                    T namedObject = xContentRegistry.parseNamedObject(categoryClass, name, parser, context);
                    if (categoryClass.equals(QueryBuilder.class)) {
                        if (namedObject instanceof TermsQueryBuilder termsQueryBuilder) {
                            if (termsQueryBuilder.termsLookup() != null) {
                                throw new IllegalArgumentException("terms query with terms lookup isn't supported as part of a role query");
                            }
                        } else if (namedObject instanceof GeoShapeQueryBuilder geoShapeQueryBuilder) {
                            if (geoShapeQueryBuilder.shape() == null) {
                                throw new IllegalArgumentException(
                                    "geoshape query referring to indexed shapes isn't supported as part of a role query"
                                );
                            }
                        }
                    }
                    return namedObject;
                }
            };
            try (XContentParser parser = XContentFactory.xContent(query).createParser(parserConfig(registryWrapper), query)) {
                return AbstractQueryBuilder.parseTopLevelQuery(parser, queryName -> {
                    switch (queryName) {
                        // actually only if percolate query is referring to an existing document then this is problematic,
                        // a normal percolate query does work. However we can't check that here as this query builder is inside
                        // another module. So we don't allow the entire percolate query. I don't think users would ever use
                        // a percolate query as role query, so this restriction shouldn't prohibit anyone from using dls.
                        case "percolate", "has_child", "has_parent" -> throw new IllegalArgumentException(
                            queryName + " query isn't supported as part of a role query"
                        );
                    }
                });
            }
        }
        return null;
    }

    private static XContentParserConfiguration parserConfig(NamedXContentRegistry xContentRegistry) {
        return XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry).withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
    }
}
