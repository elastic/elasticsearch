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
package org.elasticsearch.test.rest.test;

import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.test.rest.parser.RestTestSuiteParseContext;
import org.elasticsearch.test.rest.parser.SkipSectionParser;
import org.elasticsearch.test.rest.section.SkipSection;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SkipSectionParserTests extends AbstractParserTests {

    @Test
    public void testParseSkipSection() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                        "version:     \"0 - 0.90.2\"\n" +
                        "reason:      Delete ignores the parent param"
        );

        SkipSectionParser skipSectionParser = new SkipSectionParser();

        SkipSection skipSection = skipSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser, "0.90.7"));

        assertThat(skipSection, notNullValue());
        assertThat(skipSection.getVersion(), equalTo("0 - 0.90.2"));
        assertThat(skipSection.getReason(), equalTo("Delete ignores the parent param"));
    }

    @Test(expected = RestTestParseException.class)
    public void testParseSkipSectionNoReason() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "version:     \"0 - 0.90.2\"\n"
        );

        SkipSectionParser skipSectionParser = new SkipSectionParser();
        skipSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser, "0.90.7"));
    }

    @Test(expected = RestTestParseException.class)
    public void testParseSkipSectionNoVersion() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "reason:      Delete ignores the parent param\n"
        );

        SkipSectionParser skipSectionParser = new SkipSectionParser();
        skipSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser, "0.90.7"));
    }
}