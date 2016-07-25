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

import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.yaml.parser.ClientYamlTestParseException;
import org.elasticsearch.test.rest.yaml.parser.ClientYamlTestSuiteParseContext;
import org.elasticsearch.test.rest.yaml.parser.SetSectionParser;
import org.elasticsearch.test.rest.yaml.section.SetSection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SetSectionParserTests extends AbstractParserTestCase {
    public void testParseSetSectionSingleValue() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                        "{ _id: id }"
        );

        SetSectionParser setSectionParser = new SetSectionParser();

        SetSection setSection = setSectionParser.parse(new ClientYamlTestSuiteParseContext("api", "suite", parser));

        assertThat(setSection, notNullValue());
        assertThat(setSection.getStash(), notNullValue());
        assertThat(setSection.getStash().size(), equalTo(1));
        assertThat(setSection.getStash().get("_id"), equalTo("id"));
    }

    public void testParseSetSectionMultipleValues() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ _id: id, _type: type, _index: index }"
        );

        SetSectionParser setSectionParser = new SetSectionParser();

        SetSection setSection = setSectionParser.parse(new ClientYamlTestSuiteParseContext("api", "suite", parser));

        assertThat(setSection, notNullValue());
        assertThat(setSection.getStash(), notNullValue());
        assertThat(setSection.getStash().size(), equalTo(3));
        assertThat(setSection.getStash().get("_id"), equalTo("id"));
        assertThat(setSection.getStash().get("_type"), equalTo("type"));
        assertThat(setSection.getStash().get("_index"), equalTo("index"));
    }

    public void testParseSetSectionNoValues() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ }"
        );

        SetSectionParser setSectionParser = new SetSectionParser();
        try {
            setSectionParser.parse(new ClientYamlTestSuiteParseContext("api", "suite", parser));
            fail("Expected RestTestParseException");
        } catch (ClientYamlTestParseException e) {
            assertThat(e.getMessage(), is("set section must set at least a value"));
        }
    }
}