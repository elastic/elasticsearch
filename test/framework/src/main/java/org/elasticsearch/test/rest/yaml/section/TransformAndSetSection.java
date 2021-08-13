/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a transform_and_set section:
 * <p>
 *
 * In the following example,<br>
 * - transform_and_set: { login_creds: "#base64EncodeCredentials(user,password)" }<br>
 * user and password are from the response which are joined by ':' and Base64 encoded and then stashed as 'login_creds'
 *
 */
public class TransformAndSetSection implements ExecutableSection {
    public static TransformAndSetSection parse(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;

        TransformAndSetSection transformAndStashSection = new TransformAndSetSection(parser.getTokenLocation());

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                transformAndStashSection.addSet(currentFieldName, parser.text());
            }
        }

        parser.nextToken();

        if (transformAndStashSection.getStash().isEmpty()) {
            throw new ParsingException(transformAndStashSection.location, "transform_and_set section must set at least a value");
        }

        return transformAndStashSection;
    }

    private final Map<String, String> transformStash = new HashMap<>();
    private final XContentLocation location;

    public TransformAndSetSection(XContentLocation location) {
        this.location = location;
    }

    public void addSet(String stashedField, String transformThis) {
        transformStash.put(stashedField, transformThis);
    }

    public Map<String, String> getStash() {
        return transformStash;
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
        for (Map.Entry<String, String> entry : transformStash.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value.startsWith("#base64EncodeCredentials(") && value.endsWith(")")) {
                value = entry.getValue().substring("#base64EncodeCredentials(".length(), entry.getValue().lastIndexOf(")"));
                String[] idAndPassword = value.split(",");
                if (idAndPassword.length == 2) {
                    String credentials = executionContext.response(idAndPassword[0].trim()) + ":"
                            + executionContext.response(idAndPassword[1].trim());
                    value = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                } else {
                    throw new IllegalArgumentException("base64EncodeCredentials requires a username/id and a password parameters");
                }
            }
            executionContext.stash().stashValue(key, value);
        }
    }

}
