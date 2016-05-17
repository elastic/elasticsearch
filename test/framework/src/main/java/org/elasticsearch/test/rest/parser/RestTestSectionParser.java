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
 */
public class RestTestSectionParser implements RestTestFragmentParser<TestSection> {

    @Override
    public TestSection parse(RestTestSuiteParseContext parseContext) throws IOException, RestTestParseException {
        XContentParser parser = parseContext.parser();
        parseContext.advanceToFieldName();
        TestSection testSection = new TestSection(parser.currentName());
        try {
            parser.nextToken();
            testSection.setSkipSection(parseContext.parseSkipSection());
    
            while ( parser.currentToken() != XContentParser.Token.END_ARRAY) {
                parseContext.advanceToFieldName();
                testSection.addExecutableSection(parseContext.parseExecutableSection());
            }
    
            parser.nextToken();
            assert parser.currentToken() == XContentParser.Token.END_OBJECT;
            parser.nextToken();
    
            return testSection;
        } catch (Exception e) {
            throw new RestTestParseException("Error parsing test named [" + testSection.getName() + "]", e);
        }
    }

}
