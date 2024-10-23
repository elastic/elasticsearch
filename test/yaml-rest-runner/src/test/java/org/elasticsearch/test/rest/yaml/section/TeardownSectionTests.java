/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.xcontent.yaml.YamlXContent;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for the teardown section.
 */
public class TeardownSectionTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testParseTeardownSection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - do:
                delete:
                  index: foo
                  type: doc
                  id: 1
                  ignore: 404
            - do:
                delete2:
                  index: foo
                  type: doc
                  id: 1
                  ignore: 404
            """);

        TeardownSection section = TeardownSection.parse(parser);
        assertThat(section, notNullValue());
        assertThat(section.getPrerequisiteSection().isEmpty(), equalTo(true));
        assertThat(section.getDoSections().size(), equalTo(2));
        assertThat(((DoSection) section.getDoSections().get(0)).getApiCallSection().getApi(), equalTo("delete"));
        assertThat(((DoSection) section.getDoSections().get(1)).getApiCallSection().getApi(), equalTo("delete2"));
    }

    public void testParseWithSkip() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - skip:
                cluster_features:  "some_feature"
                reason:   "there is a reason"
            - do:
                delete:
                  index: foo
                  type: doc
                  id: 1
                  ignore: 404
            - do:
                delete2:
                  index: foo
                  type: doc
                  id: 1
                  ignore: 404
            """);

        TeardownSection section = TeardownSection.parse(parser);
        assertThat(section, notNullValue());
        assertThat(section.getPrerequisiteSection().isEmpty(), equalTo(false));
        assertThat(section.getPrerequisiteSection().skipReason, equalTo("there is a reason"));
        assertThat(section.getDoSections().size(), equalTo(2));
        assertThat(((DoSection) section.getDoSections().get(0)).getApiCallSection().getApi(), equalTo("delete"));
        assertThat(((DoSection) section.getDoSections().get(1)).getApiCallSection().getApi(), equalTo("delete2"));
    }

    public void testParseWithRequires() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            - requires:
                cluster_features:  "some_feature"
                reason:   "there is a reason"
            - do:
                delete:
                  index: foo
                  type: doc
                  id: 1
                  ignore: 404
            - do:
                delete2:
                  index: foo
                  type: doc
                  id: 1
                  ignore: 404
            """);

        TeardownSection section = TeardownSection.parse(parser);
        assertThat(section, notNullValue());
        assertThat(section.getPrerequisiteSection().isEmpty(), equalTo(false));
        assertThat(section.getPrerequisiteSection().requireReason, equalTo("there is a reason"));
        assertThat(section.getDoSections().size(), equalTo(2));
        assertThat(((DoSection) section.getDoSections().get(0)).getApiCallSection().getApi(), equalTo("delete"));
        assertThat(((DoSection) section.getDoSections().get(1)).getApiCallSection().getApi(), equalTo("delete2"));
    }
}
