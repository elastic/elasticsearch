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
package org.elasticsearch.test.rest.parser;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.section.TestSection;

import java.io.IOException;

/**
 * Parser for a complete test section
 *
 * Depending on the elasticsearch version the tests are going to run against, test sections might need to get skipped
 * In that case the relevant test sections parsing is entirely skipped
 */
public class RestTestSectionParser implements RestTestFragmentParser<TestSection> {

    @Override
    public TestSection parse(RestTestSuiteParseContext parseContext) throws IOException, RestTestParseException {
        XContentParser parser = parseContext.parser();
        parseContext.advanceToFieldName();
        TestSection testSection = new TestSection(parser.currentName());
        parser.nextToken();
        testSection.setSkipSection(parseContext.parseSkipSection());

        boolean skip = testSection.getSkipSection().skipVersion(parseContext.getCurrentVersion());

        while ( parser.currentToken() != XContentParser.Token.END_ARRAY) {
            if (skip) {
                //if there was a skip section, there was a setup section as well, which means that we are sure
                // the current token is at the beginning of a new object
                assert parser.currentToken() == XContentParser.Token.START_OBJECT;
                //we need to be at the beginning of an object to be able to skip children
                parser.skipChildren();
                //after skipChildren we are at the end of the skipped object, need to move on
                parser.nextToken();
            } else {
                parseContext.advanceToFieldName();
                testSection.addExecutableSection(parseContext.parseExecutableSection());
            }
        }

        parser.nextToken();
        assert parser.currentToken() == XContentParser.Token.END_OBJECT;
        parser.nextToken();

        return testSection;
    }

}
