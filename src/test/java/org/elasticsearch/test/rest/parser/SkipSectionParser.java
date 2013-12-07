/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.section.SkipSection;

import java.io.IOException;

/**
 * Parser for skip sections
 */
public class SkipSectionParser implements RestTestFragmentParser<SkipSection> {

    @Override
    public SkipSection parse(RestTestSuiteParseContext parseContext) throws IOException, RestTestParseException {

        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        XContentParser.Token token;
        String version = null;
        String reason = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    version = parser.text();
                } else if ("reason".equals(currentFieldName)) {
                    reason = parser.text();
                } else {
                    throw new RestTestParseException("field " + currentFieldName + " not supported within skip section");
                }
            }
        }

        parser.nextToken();

        if (!Strings.hasLength(version)) {
            throw new RestTestParseException("version is mandatory within skip section");
        }
        if (!Strings.hasLength(reason)) {
            throw new RestTestParseException("reason is mandatory within skip section");
        }

        return new SkipSection(version, reason);

    }
}
