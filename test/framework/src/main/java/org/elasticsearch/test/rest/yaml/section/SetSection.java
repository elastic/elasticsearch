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
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a set section:
 *
 *   - set: {_scroll_id: scroll_id}
 *
 */
public class SetSection implements ExecutableSection {
    public static SetSection parse(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;

        SetSection setSection = new SetSection(parser.getTokenLocation());

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                setSection.addSet(currentFieldName, parser.text());
            }
        }

        parser.nextToken();

        if (setSection.getStash().isEmpty()) {
            throw new ParsingException(setSection.location, "set section must set at least a value");
        }

        return setSection;
    }

    private final Map<String, String> stash = new HashMap<>();
    private final XContentLocation location;

    public SetSection(XContentLocation location) {
        this.location = location;
    }

    public void addSet(String responseField, String stashedField) {
        stash.put(responseField, stashedField);
    }

    public Map<String, String> getStash() {
        return stash;
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
        for (Map.Entry<String, String> entry : stash.entrySet()) {
            Object actualValue = executionContext.response(entry.getKey());
            executionContext.stash().stashValue(entry.getValue(), actualValue);
        }
    }
}
