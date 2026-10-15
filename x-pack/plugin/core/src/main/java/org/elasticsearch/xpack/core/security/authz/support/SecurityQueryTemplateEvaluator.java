/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
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
import java.util.Map;

/**
 * Helper class that helps to evaluate the query source template.
 */
public final class SecurityQueryTemplateEvaluator {

    /** The template variable that exposes the user's profile uid; see {@link #evaluateTemplate(String, ScriptService, User, String)}. */
    public static final String PROFILE_UID_TEMPLATE_VARIABLE = "_user.profile_uid";

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
        return evaluateTemplate(querySource, scriptService, user, null);
    }

    /**
     * As {@link #evaluateTemplate(String, ScriptService, User)}, additionally exposing {@code profileUid} as
     * {@code _user.profile_uid}. The variable is absent, and so renders empty, when {@code profileUid} is {@code null}.
     * The uid is resolved during authorization and carried in the thread context as
     * {@code AuthorizationServiceField.PROFILE_UID_VALUE}.
     */
    public static String evaluateTemplate(
        final String querySource,
        final ScriptService scriptService,
        final User user,
        @Nullable final String profileUid
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
                if (profileUid != null) {
                    userModel.put("profile_uid", profileUid);
                }
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
        return q -> SecurityQueryTemplateEvaluator.evaluateTemplate(q.utf8ToString(), scriptService, user);
    }

    public static DlsQueryEvaluationContext wrap(User user, @Nullable String profileUid, ScriptService scriptService) {
        return q -> SecurityQueryTemplateEvaluator.evaluateTemplate(q.utf8ToString(), scriptService, user, profileUid);
    }

    @FunctionalInterface
    public interface DlsQueryEvaluationContext {
        String evaluate(BytesReference query);
    }

}
