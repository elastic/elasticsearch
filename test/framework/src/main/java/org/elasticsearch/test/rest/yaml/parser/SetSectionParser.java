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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.yaml.section.SetSection;

import java.io.IOException;

/**
 * Parser for set sections
 */
public class SetSectionParser implements ClientYamlTestFragmentParser<SetSection> {

    @Override
    public SetSection parse(ClientYamlTestSuiteParseContext parseContext) throws IOException, ClientYamlTestParseException {

        XContentParser parser = parseContext.parser();

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
            throw new ClientYamlTestParseException("set section must set at least a value");
        }

        return setSection;
    }
}
