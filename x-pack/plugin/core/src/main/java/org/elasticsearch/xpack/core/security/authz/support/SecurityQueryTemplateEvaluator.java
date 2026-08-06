/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
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
import java.util.Set;
import java.util.TreeSet;

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
        return evaluateTemplate(querySource, scriptService, user, Map.of(), Map.of());
    }

    /**
     * Same as {@link #evaluateTemplate(String, ScriptService, User)}, but also exposes the user's
     * application privileges to the template: {@code _user.applications} /
     * {@code _user.application_resources} (the resource patterns, e.g. the Kibana spaces the user can
     * access) and {@code _user.application_privileges} (the resource-scoped {@code <resource>|<action>}
     * grants). Used so a DLS query can filter documents by the resources and/or actions a user holds.
     *
     * @param applicationResources applicationName -&gt; granted resource patterns
     * @param applicationPrivileges applicationName -&gt; {@code <resource>|<action>} grant tuples
     * Both must be deterministically ordered so the rendered query (and any cache key derived from it)
     * is stable across render sites.
     */
    public static String evaluateTemplate(
        final String querySource,
        final ScriptService scriptService,
        final User user,
        final Map<String, List<String>> applicationResources,
        final Map<String, List<String>> applicationPrivileges
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
                Map<String, Object> userModel = new HashMap<>();
                userModel.put("username", user.principal());
                userModel.put("full_name", user.fullName());
                userModel.put("email", user.email());
                userModel.put("roles", Arrays.asList(user.roles()));
                userModel.put("metadata", Collections.unmodifiableMap(user.metadata()));
                userModel.put("applications", Collections.unmodifiableMap(applicationResources));
                // Flattened, deterministically-ordered union of every application's resources. Provided
                // because a mustache template cannot path-navigate an application name that contains a
                // '.' (e.g. Kibana's "kibana-.kibana"), so {{_user.applications.kibana-.kibana}} does not
                // resolve; {{#toJson}}_user.application_resources{{/toJson}} gives a usable list instead.
                final Set<String> flattenedResources = new TreeSet<>();
                for (List<String> resources : applicationResources.values()) {
                    flattenedResources.addAll(resources);
                }
                userModel.put("application_resources", List.copyOf(flattenedResources));
                // Flattened union of resource-scoped "<resource>|<action>" grants, for DLS queries that
                // need to enforce space AND privilege together (e.g. only KIs whose required action the
                // user holds in that KI's space).
                final Set<String> flattenedPrivileges = new TreeSet<>();
                for (List<String> grants : applicationPrivileges.values()) {
                    flattenedPrivileges.addAll(grants);
                }
                userModel.put("application_privileges", List.copyOf(flattenedPrivileges));
                Map<String, Object> extraParams = Collections.singletonMap("_user", userModel);

                return MustacheTemplateEvaluator.evaluate(scriptService, parser, extraParams);
            } else {
                return querySource;
            }
        } catch (IOException ioe) {
            throw new ElasticsearchParseException("failed to parse query", ioe);
        }
    }

    public static DlsQueryEvaluationContext wrap(User user, ScriptService scriptService) {
        return wrap(user, scriptService, Map.of(), Map.of());
    }

    public static DlsQueryEvaluationContext wrap(
        User user,
        ScriptService scriptService,
        Map<String, List<String>> applicationResources,
        Map<String, List<String>> applicationPrivileges
    ) {
        return q -> SecurityQueryTemplateEvaluator.evaluateTemplate(
            q.utf8ToString(),
            scriptService,
            user,
            applicationResources,
            applicationPrivileges
        );
    }

    @FunctionalInterface
    public interface DlsQueryEvaluationContext {
        String evaluate(BytesReference query);
    }

}
