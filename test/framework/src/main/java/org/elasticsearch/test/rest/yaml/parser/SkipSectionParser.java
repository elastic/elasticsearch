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
package org.elasticsearch.test.rest.yaml.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.yaml.section.SkipSection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for skip sections
 */
public class SkipSectionParser implements ClientYamlTestFragmentParser<SkipSection> {

    @Override
    public SkipSection parse(ClientYamlTestSuiteParseContext parseContext) throws IOException, ClientYamlTestParseException {

        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        XContentParser.Token token;
        String version = null;
        String reason = null;
        List<String> features = new ArrayList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    version = parser.text();
                } else if ("reason".equals(currentFieldName)) {
                    reason = parser.text();
                } else if ("features".equals(currentFieldName)) {
                    features.add(parser.text());
                }
                else {
                    throw new ClientYamlTestParseException("field " + currentFieldName + " not supported within skip section");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("features".equals(currentFieldName)) {
                    while(parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        features.add(parser.text());
                    }
                }
            }
        }

        parser.nextToken();

        if (!Strings.hasLength(version) && features.isEmpty()) {
            throw new ClientYamlTestParseException("version or features is mandatory within skip section");
        }
        if (Strings.hasLength(version) && !features.isEmpty()) {
            throw new ClientYamlTestParseException("version or features are mutually exclusive");
        }
        if (Strings.hasLength(version) && !Strings.hasLength(reason)) {
            throw new ClientYamlTestParseException("reason is mandatory within skip version section");
        }

        return new SkipSection(version, features, reason);

    }
}
