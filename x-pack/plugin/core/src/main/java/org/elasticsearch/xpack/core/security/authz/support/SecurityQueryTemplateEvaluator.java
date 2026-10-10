/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.support.MustacheTemplateEvaluator;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class that helps to evaluate the query source template.
 */
public final class SecurityQueryTemplateEvaluator {

    private SecurityQueryTemplateEvaluator() {}

    /**
     * If the query source is a template, then parses the script, compiles the
     * script with user details parameters and then executes it to return the
     * query string.
     * <p>
     * Note: This method always enforces "mustache" script language for the
     * template.
     *
     * @param querySource query string template to be evaluated.
     * @param scriptService {@link ScriptService}
     * @param user {@link User} details for user defined parameters in the
     * script.
     * @return resultant query string after compiling and executing the script.
     * If the source does not contain template then it will return the query
     * source without any modifications.
     */
    public static String evaluateTemplate(final String querySource, final ScriptService scriptService, final User user) {
        return evaluateTemplate(querySource, scriptService, user, ResolvedDlsLookups.EMPTY);
    }

    /**
     * Like {@link #evaluateTemplate(String, ScriptService, User)} but also exposes the values of the {@link DlsLookup}s the
     * template declares under {@code _lookup.<name>}. Every declared lookup must be present in {@code resolvedLookups}; the
     * coordinating node resolves them before dispatching the action, so a missing value indicates the template is being
     * evaluated outside an authorized request and evaluation fails rather than rendering against missing data.
     *
     * @param querySource     query string template to be evaluated
     * @param scriptService   {@link ScriptService}
     * @param user            {@link User} details for user defined parameters in the script
     * @param resolvedLookups the values resolved for this request's lookups
     * @return resultant query string after compiling and executing the script, or the unmodified source if it is not a template
     */
    public static String evaluateTemplate(
        final String querySource,
        final ScriptService scriptService,
        final User user,
        final ResolvedDlsLookups resolvedLookups
    ) {
        // EMPTY is safe here because we never use namedObject
        try (XContentParser parser = XContentFactory.xContent(querySource).createParser(XContentParserConfiguration.EMPTY, querySource)) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("Unexpected token [" + token + "]");
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ElasticsearchParseException("Unexpected token [" + token + "]");
            }
            if ("template".equals(parser.currentName())) {
                token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException("Unexpected token [" + token + "]");
                }
                final Script script = Script.parse(parser);
                final List<DlsLookup> lookups = DlsLookup.parseSiblingsOfTemplate(parser);

                Map<String, Object> userModel = new HashMap<>();
                userModel.put("username", user.principal());
                userModel.put("full_name", user.fullName());
                userModel.put("email", user.email());
                userModel.put("roles", Arrays.asList(user.roles()));
                userModel.put("metadata", Collections.unmodifiableMap(user.metadata()));
                Map<String, Object> extraParams = new HashMap<>();
                extraParams.put("_user", userModel);
                if (lookups.isEmpty() == false) {
                    // only templates that declare lookups see the namespace, so existing templates render with unchanged params
                    extraParams.put("_lookup", lookupModel(lookups, resolvedLookups));
                }

                return MustacheTemplateEvaluator.evaluate(scriptService, MustacheTemplateEvaluator.withExtraParams(script, extraParams));
            } else {
                return querySource;
            }
        } catch (IOException ioe) {
            throw new ElasticsearchParseException("failed to parse query", ioe);
        }
    }

    private static Map<String, Object> lookupModel(List<DlsLookup> lookups, ResolvedDlsLookups resolvedLookups) {
        final Map<String, Object> model = new HashMap<>();
        for (DlsLookup lookup : lookups) {
            if (resolvedLookups.contains(lookup) == false) {
                throw new IllegalStateException(
                    "DLS lookup [" + lookup.name() + "] of type [" + lookup.type() + "] has not been resolved for this request"
                );
            }
            model.put(lookup.name(), resolvedLookups.get(lookup));
        }
        return Collections.unmodifiableMap(model);
    }

    public static DlsQueryEvaluationContext wrap(User user, ScriptService scriptService) {
        return wrap(user, scriptService, ResolvedDlsLookups.EMPTY);
    }

    public static DlsQueryEvaluationContext wrap(User user, ScriptService scriptService, ResolvedDlsLookups resolvedLookups) {
        return q -> SecurityQueryTemplateEvaluator.evaluateTemplate(q.utf8ToString(), scriptService, user, resolvedLookups);
    }

    @FunctionalInterface
    public interface DlsQueryEvaluationContext {
        String evaluate(BytesReference query);
    }

}
