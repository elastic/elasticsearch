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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.MapsTests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DynamicTemplate.MatchType;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class DynamicTemplateTests extends ESTestCase {

    public void testParseUnknownParam() throws Exception {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        templateDef.put("random_param", "random_value");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> DynamicTemplate.parse("my_template", templateDef));
        assertEquals("Illegal dynamic template parameter: [random_param]", e.getMessage());
    }

    public void testParseUnknownMatchType() {
        Map<String, Object> templateDef2 = new HashMap<>();
        templateDef2.put("match_mapping_type", "text");
        templateDef2.put("mapping", Collections.singletonMap("store", true));
        // if a wrong match type is specified, we ignore the template
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> DynamicTemplate.parse("my_template", templateDef2));
        assertEquals("No field type matched on [text], possible values are [object, string, long, double, boolean, date, binary]",
                e.getMessage());
    }

    public void testParseInvalidRegex() {
        for (String param : new String[] { "path_match", "match", "path_unmatch", "unmatch" }) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", "foo");
            templateDef.put(param, "*a");
            templateDef.put("match_pattern", "regex");
            templateDef.put("mapping", Collections.singletonMap("store", true));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> DynamicTemplate.parse("my_template", templateDef));
            assertEquals("Pattern [*a] of type [regex] is invalid. Cannot create dynamic template [my_template].", e.getMessage());
        }
    }

    public void testMatchAllTemplate() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "*");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.match("a.b", "b", randomFrom(XContentFieldType.values())));
    }

    public void testMatchTypeTemplate() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.match("a.b", "b", XContentFieldType.STRING));
        assertFalse(template.match("a.b", "b", XContentFieldType.BOOLEAN));
    }

    public void testSerialization() throws Exception {
        // type-based template
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        XContentBuilder builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"match_mapping_type\":\"string\",\"mapping\":{\"store\":true}}", Strings.toString(builder));

        // name-based template
        templateDef = new HashMap<>();
        templateDef.put("match", "*name");
        templateDef.put("unmatch", "first_name");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"match\":\"*name\",\"unmatch\":\"first_name\",\"mapping\":{\"store\":true}}", Strings.toString(builder));

        // path-based template
        templateDef = new HashMap<>();
        templateDef.put("path_match", "*name");
        templateDef.put("path_unmatch", "first_name");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"path_match\":\"*name\",\"path_unmatch\":\"first_name\",\"mapping\":{\"store\":true}}",
                Strings.toString(builder));

        // regex matching
        templateDef = new HashMap<>();
        templateDef.put("match", "^a$");
        templateDef.put("match_pattern", "regex");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"match\":\"^a$\",\"match_pattern\":\"regex\",\"mapping\":{\"store\":true}}", Strings.toString(builder));
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(randomTemplate(), original -> new DynamicTemplate(original), original -> {
            String name = original.name();
            String pathMatch = original.getPathMatch();
            String pathUnmatch = original.getPathUnmatch();
            String match = original.getMatch();
            String unmatch = original.getUnmatch();
            XContentFieldType xContentFieldType = original.getXcontentFieldType();
            MatchType matchType = original.getMatchType();
            Map<String, Object> mapping = original.getMapping();
            int switchCase = randomInt(7);
            switch (switchCase) {
            case 0:
                name = name + randomAlphaOfLength(3);
                break;
            case 1:
                pathMatch = pathMatch + randomAlphaOfLength(3);
                break;
            case 2:
                pathUnmatch = pathUnmatch + randomAlphaOfLength(3);
                break;
            case 3:
                match = match + randomAlphaOfLength(3);
                break;
            case 4:
                unmatch = unmatch + randomAlphaOfLength(3);
                break;
            case 5:
                xContentFieldType = randomValueOtherThan(xContentFieldType, () -> randomFrom(XContentFieldType.values()));
                break;
            case 6:
                matchType = randomValueOtherThan(matchType, () -> randomFrom(MatchType.values()));
                break;
            case 7:
                mapping = new HashMap<>(mapping);
                mapping.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
                break;
            }
            return new DynamicTemplate(name, pathMatch, pathUnmatch , match, unmatch, xContentFieldType, matchType, mapping);
        });
    }

    private DynamicTemplate randomTemplate() {
        String name = randomAlphaOfLengthBetween(4, 8);
        String pathMatch = randomAlphaOfLengthBetween(4, 8);
        String pathUnmatch = randomAlphaOfLengthBetween(4, 8);
        String match = randomAlphaOfLengthBetween(4, 8);
        String unmatch = randomAlphaOfLengthBetween(4, 8);
        XContentFieldType xContentFieldType = randomFrom(XContentFieldType.values());
        MatchType matchType = randomFrom(MatchType.values());
        Map<String, Object> mapping = MapsTests.randomMap(randomIntBetween(0, 10), () -> ESTestCase.randomAlphaOfLength(5),
                () -> ESTestCase.randomAlphaOfLength(5));
        return new DynamicTemplate(name, pathMatch, pathUnmatch , match, unmatch, xContentFieldType, matchType, mapping);
    }
}
