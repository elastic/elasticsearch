/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
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

    private SecurityQueryTemplateEvaluator() {
    }

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
     * @throws IOException thrown when there is any error parsing the query
     * string.
     */
    public static String evaluateTemplate(final String querySource, final ScriptService scriptService, final User user) throws IOException {
        // EMPTY is safe here because we never use namedObject
        try (XContentParser parser = XContentFactory.xContent(querySource).createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, querySource)) {
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
                Script script = Script.parse(parser);
                // Add the user details to the params
                Map<String, Object> params = new HashMap<>();
                if (script.getParams() != null) {
                    params.putAll(script.getParams());
                }
                Map<String, Object> userModel = new HashMap<>();
                userModel.put("username", user.principal());
                userModel.put("full_name", user.fullName());
                userModel.put("email", user.email());
                userModel.put("roles", Arrays.asList(user.roles()));
                userModel.put("metadata", Collections.unmodifiableMap(user.metadata()));
                params.put("_user", userModel);
                // Always enforce mustache script lang:
                script = new Script(script.getType(), script.getType() == ScriptType.STORED ? null : "mustache", script.getIdOrCode(),
                        script.getOptions(), params);
                TemplateScript compiledTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(script.getParams());
                return compiledTemplate.execute();
            } else {
                return querySource;
            }
        }
    }
}
