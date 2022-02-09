/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SetSectionTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testParseSetSectionSingleValue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ _id: id }");

        SetSection setSection = SetSection.parse(parser);
        assertThat(setSection, notNullValue());
        assertThat(setSection.getStash(), notNullValue());
        assertThat(setSection.getStash().size(), equalTo(1));
        assertThat(setSection.getStash().get("_id"), equalTo("id"));
    }

    public void testParseSetSectionMultipleValues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ _id: id, _type: type, _index: index }");

        SetSection setSection = SetSection.parse(parser);
        assertThat(setSection, notNullValue());
        assertThat(setSection.getStash(), notNullValue());
        assertThat(setSection.getStash().size(), equalTo(3));
        assertThat(setSection.getStash().get("_id"), equalTo("id"));
        assertThat(setSection.getStash().get("_type"), equalTo("type"));
        assertThat(setSection.getStash().get("_index"), equalTo("index"));
    }

    public void testParseSetSectionNoValues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ }");

        Exception e = expectThrows(ParsingException.class, () -> SetSection.parse(parser));
        assertThat(e.getMessage(), is("set section must set at least a value"));
    }
}
