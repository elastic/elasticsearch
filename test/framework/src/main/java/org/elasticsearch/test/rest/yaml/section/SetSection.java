/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
