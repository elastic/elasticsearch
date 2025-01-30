/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

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
            if (isBase64EncodeCredentials(value)) {
                String[] params = value.substring("#base64EncodeCredentials(".length(), value.lastIndexOf(')')).split(",");
                if (params.length != 2) {
                    throw new IllegalArgumentException("base64EncodeCredentials requires a username/id and a password parameters");
                }
                String credentials = executionContext.response(params[0].trim()) + ":" + executionContext.response(params[1].trim());
                executionContext.stash().stashValue(key, base64Encode(credentials));

            } else if (isBase64Encode(value)) {
                String param = value.substring("#base64Encode(".length(), value.lastIndexOf(')'));
                executionContext.stash().stashValue(key, base64Encode(param));

            } else {
                // behaves like a normal set if the value does not contain a #base64EncodeCredentials or #base64Encode
                executionContext.stash().stashValue(key, value);
            }
        }
    }

    /**
     * The {@code #base64EncodeCredentials()} function is used to encode the credentials in the format of e.g. {@code username:password}.
     * It accepts two parameters, whose values are looked up from the response, encoded and then stashed in the current execution context.
     * @return true if the transform value is of the form {@code #base64EncodeCredentials(param1, param2)}
     */
    private static boolean isBase64EncodeCredentials(String value) {
        return value.startsWith("#base64EncodeCredentials(") && value.endsWith(")");
    }

    /**
     * The {@code #base64Encode} function is used to encode the given string value as is.
     * @return true if the transform value is of the form {@code #base64Encode()}
     */
    private static boolean isBase64Encode(String value) {
        return value.startsWith("#base64Encode(") && value.endsWith(")");
    }

    private static String base64Encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
