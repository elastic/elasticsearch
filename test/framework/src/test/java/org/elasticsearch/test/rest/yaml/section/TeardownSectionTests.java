/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        assertThat(((DoSection)section.getDoSections().get(0)).getApiCallSection().getApi(), equalTo("delete"));
        assertThat(((DoSection)section.getDoSections().get(1)).getApiCallSection().getApi(), equalTo("delete2"));
    }

    public void testParseWithSkip() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "  - skip:\n" +
                        "      version:  \"6.0.0 - 6.3.0\"\n" +
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
        assertThat(section.getSkipSection().getLowerVersion(), equalTo(Version.fromString("6.0.0")));
        assertThat(section.getSkipSection().getUpperVersion(), equalTo(Version.fromString("6.3.0")));
        assertThat(section.getSkipSection().getReason(), equalTo("there is a reason"));
        assertThat(section.getDoSections().size(), equalTo(2));
        assertThat(((DoSection)section.getDoSections().get(0)).getApiCallSection().getApi(), equalTo("delete"));
        assertThat(((DoSection)section.getDoSections().get(1)).getApiCallSection().getApi(), equalTo("delete2"));
    }
}
