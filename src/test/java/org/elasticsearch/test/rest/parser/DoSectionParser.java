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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.section.ApiCallSection;
import org.elasticsearch.test.rest.section.DoSection;

import java.io.IOException;
import java.util.Map;

/**
 * Parser for do sections
 */
public class DoSectionParser implements RestTestFragmentParser<DoSection> {

    @Override
    public DoSection parse(RestTestSuiteParseContext parseContext) throws IOException, RestTestParseException {

        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        XContentParser.Token token;

        DoSection doSection = new DoSection();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("catch".equals(currentFieldName)) {
                    doSection.setCatch(parser.text());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (currentFieldName != null) {
                    ApiCallSection apiCallSection = new ApiCallSection(currentFieldName);
                    String paramName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            paramName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("body".equals(paramName)) {
                                apiCallSection.addBody(parser.text());
                            } else {
                                apiCallSection.addParam(paramName, parser.text());
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("body".equals(paramName)) {
                                Map<String,Object> map = parser.mapOrdered();
                                XContentBuilder contentBuilder = XContentFactory.jsonBuilder().map(map);
                                apiCallSection.addBody(contentBuilder.string());
                            }
                        }
                    }
                    doSection.setApiCallSection(apiCallSection);
                }
            }
        }

        parser.nextToken();

        if (doSection.getApiCallSection() == null) {
            throw new RestTestParseException("client call section is mandatory within a do section");
        }

        return doSection;
    }
}
