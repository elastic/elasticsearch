/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.template;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class XContentTemplateTests extends ElasticsearchTestCase {

    @Test
    public void testYaml() throws Exception {
        Map<String, Object> model = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        a.put("aa", "aa");
        a.put("ab", "ab");
        model.put("a", a);
        Map<String, Object> b = new HashMap<>();
        b.put("ba", 21);
        model.put("b", b);
        model.put("c", "c");

        String text = XContentTemplate.YAML.render(model);

        // now lets read it as xcontent and make sure it has all the pieces
        XContentParser parser = YamlXContent.yamlXContent.createParser(new BytesArray(text));
        XContentParser.Token token = parser.nextToken();
        assertThat(token, is(XContentParser.Token.START_OBJECT));
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("a".equals(currentFieldName)) {
                assertThat(token, is(XContentParser.Token.START_OBJECT));
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        assertThat(token, is(XContentParser.Token.VALUE_STRING));
                        if ("aa".equals(currentFieldName)) {
                            assertThat(parser.text(), equalTo("aa"));
                        } else if ("ab".equals(currentFieldName)) {
                            assertThat(parser.text(), equalTo("ab"));
                        } else {
                            fail("unexpected field name [" + currentFieldName + "]");
                        }
                    }
                }
            } else if ("b".equals(currentFieldName)) {
                assertThat(token, is(XContentParser.Token.START_OBJECT));
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        assertThat(token, is(XContentParser.Token.VALUE_NUMBER));
                        assertThat(currentFieldName, equalTo("ba"));
                        assertThat(parser.intValue(), is(21));
                    }
                }
            } else if ("c".equals(currentFieldName)) {
                assertThat(token, is(XContentParser.Token.VALUE_STRING));
                assertThat(parser.text(), equalTo("c"));
            } else {
                fail("unexpected field [" + currentFieldName + "]");
            }
        }
    }
}
