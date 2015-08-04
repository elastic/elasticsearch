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

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.parser.RestTestSuiteParseContext;
import org.elasticsearch.test.rest.parser.SetupSectionParser;
import org.elasticsearch.test.rest.section.SetupSection;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SetupSectionParserTests extends AbstractParserTestCase {

    @Test
    public void testParseSetupSection() throws Exception {

        parser = YamlXContent.yamlXContent.createParser(
                "  - do:\n" +
                "      index1:\n" +
                "        index:  test_1\n" +
                "        type:   test\n" +
                "        id:     1\n" +
                "        body:   { \"include\": { \"field1\": \"v1\", \"field2\": \"v2\" }, \"count\": 1 }\n" +
                "  - do:\n" +
                "      index2:\n" +
                "        index:  test_1\n" +
                "        type:   test\n" +
                "        id:     2\n" +
                "        body:   { \"include\": { \"field1\": \"v1\", \"field2\": \"v2\" }, \"count\": 1 }\n"
        );

        SetupSectionParser setupSectionParser = new SetupSectionParser();
        SetupSection setupSection = setupSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(setupSection, notNullValue());
        assertThat(setupSection.getSkipSection().isEmpty(), equalTo(true));
        assertThat(setupSection.getDoSections().size(), equalTo(2));
        assertThat(setupSection.getDoSections().get(0).getApiCallSection().getApi(), equalTo("index1"));
        assertThat(setupSection.getDoSections().get(1).getApiCallSection().getApi(), equalTo("index2"));
    }

    @Test
    public void testParseSetupAndSkipSectionNoSkip() throws Exception {

        parser = YamlXContent.yamlXContent.createParser(
                "  - skip:\n" +
                        "      version:  \"0.90.0 - 0.90.7\"\n" +
                        "      reason:   \"Update doesn't return metadata fields, waiting for #3259\"\n" +
                        "  - do:\n" +
                        "      index1:\n" +
                        "        index:  test_1\n" +
                        "        type:   test\n" +
                        "        id:     1\n" +
                        "        body:   { \"include\": { \"field1\": \"v1\", \"field2\": \"v2\" }, \"count\": 1 }\n" +
                        "  - do:\n" +
                        "      index2:\n" +
                        "        index:  test_1\n" +
                        "        type:   test\n" +
                        "        id:     2\n" +
                        "        body:   { \"include\": { \"field1\": \"v1\", \"field2\": \"v2\" }, \"count\": 1 }\n"
        );

        SetupSectionParser setupSectionParser = new SetupSectionParser();
        SetupSection setupSection = setupSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(setupSection, notNullValue());
        assertThat(setupSection.getSkipSection().isEmpty(), equalTo(false));
        assertThat(setupSection.getSkipSection(), notNullValue());
        assertThat(setupSection.getSkipSection().getLowerVersion(), equalTo(Version.V_0_90_0));
        assertThat(setupSection.getSkipSection().getUpperVersion(), equalTo(Version.V_0_90_7));
        assertThat(setupSection.getSkipSection().getReason(), equalTo("Update doesn't return metadata fields, waiting for #3259"));
        assertThat(setupSection.getDoSections().size(), equalTo(2));
        assertThat(setupSection.getDoSections().get(0).getApiCallSection().getApi(), equalTo("index1"));
        assertThat(setupSection.getDoSections().get(1).getApiCallSection().getApi(), equalTo("index2"));
    }
}
