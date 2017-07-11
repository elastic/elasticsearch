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

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for the teardown section.
 */
public class TeardownSectionTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testParseTeardownSection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "  - do:\n" +
                "      delete:\n" +
                "        index: foo\n" +
                "        type: doc\n" +
                "        id: 1\n" +
                "        ignore: 404\n" +
                "  - do:\n" +
                "      delete2:\n" +
                "        index: foo\n" +
                "        type: doc\n" +
                "        id: 1\n" +
                "        ignore: 404"
        );

        TeardownSection section = TeardownSection.parse(parser);
        assertThat(section, notNullValue());
        assertThat(section.getSkipSection().isEmpty(), equalTo(true));
        assertThat(section.getDoSections().size(), equalTo(2));
        assertThat(section.getDoSections().get(0).getApiCallSection().getApi(), equalTo("delete"));
        assertThat(section.getDoSections().get(1).getApiCallSection().getApi(), equalTo("delete2"));
    }

    public void testParseWithSkip() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "  - skip:\n" +
                        "      version:  \"5.0.0 - 5.3.0\"\n" +
                "      reason:   \"there is a reason\"\n" +
                "  - do:\n" +
                "      delete:\n" +
                "        index: foo\n" +
                "        type: doc\n" +
                "        id: 1\n" +
                "        ignore: 404\n" +
                "  - do:\n" +
                "      delete2:\n" +
                "        index: foo\n" +
                "        type: doc\n" +
                "        id: 1\n" +
                "        ignore: 404"
        );

        TeardownSection section = TeardownSection.parse(parser);
        assertThat(section, notNullValue());
        assertThat(section.getSkipSection().isEmpty(), equalTo(false));
        assertThat(section.getSkipSection().getLowerVersion(), equalTo(Version.V_5_0_0));
        assertThat(section.getSkipSection().getUpperVersion(), equalTo(Version.V_5_3_0));
        assertThat(section.getSkipSection().getReason(), equalTo("there is a reason"));
        assertThat(section.getDoSections().size(), equalTo(2));
        assertThat(section.getDoSections().get(0).getApiCallSection().getApi(), equalTo("delete"));
        assertThat(section.getDoSections().get(1).getApiCallSection().getApi(), equalTo("delete2"));
    }
}
