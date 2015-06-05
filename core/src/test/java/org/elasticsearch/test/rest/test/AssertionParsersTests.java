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
import org.elasticsearch.test.rest.parser.*;
import org.elasticsearch.test.rest.section.*;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class AssertionParsersTests extends AbstractParserTests {

    @Test
    public void testParseIsTrue() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "get.fields._timestamp"
        );

        IsTrueParser isTrueParser = new IsTrueParser();
        IsTrueAssertion trueAssertion = isTrueParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(trueAssertion, notNullValue());
        assertThat(trueAssertion.getField(), equalTo("get.fields._timestamp"));
    }

    @Test
    public void testParseIsFalse() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "docs.1._source"
        );

        IsFalseParser isFalseParser = new IsFalseParser();
        IsFalseAssertion falseAssertion = isFalseParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(falseAssertion, notNullValue());
        assertThat(falseAssertion.getField(), equalTo("docs.1._source"));
    }

    @Test
    public void testParseGreaterThan() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ field: 3}"
        );

        GreaterThanParser greaterThanParser = new GreaterThanParser();
        GreaterThanAssertion greaterThanAssertion = greaterThanParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        assertThat(greaterThanAssertion, notNullValue());
        assertThat(greaterThanAssertion.getField(), equalTo("field"));
        assertThat(greaterThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) greaterThanAssertion.getExpectedValue(), equalTo(3));
    }

    @Test
    public void testParseLessThan() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ field: 3}"
        );

        LessThanParser lessThanParser = new LessThanParser();
        LessThanAssertion lessThanAssertion = lessThanParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        assertThat(lessThanAssertion, notNullValue());
        assertThat(lessThanAssertion.getField(), equalTo("field"));
        assertThat(lessThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lessThanAssertion.getExpectedValue(), equalTo(3));
    }

    @Test
    public void testParseLength() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ _id: 22}"
        );

        LengthParser lengthParser = new LengthParser();
        LengthAssertion lengthAssertion = lengthParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        assertThat(lengthAssertion, notNullValue());
        assertThat(lengthAssertion.getField(), equalTo("_id"));
        assertThat(lengthAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lengthAssertion.getExpectedValue(), equalTo(22));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseMatchSimpleIntegerValue() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ field: 10 }"
        );

        MatchParser matchParser = new MatchParser();
        MatchAssertion matchAssertion = matchParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("field"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) matchAssertion.getExpectedValue(), equalTo(10));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseMatchSimpleStringValue() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ foo: bar }"
        );

        MatchParser matchParser = new MatchParser();
        MatchAssertion matchAssertion = matchParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("foo"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(String.class));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("bar"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseMatchArray() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{'matches': ['test_percolator_1', 'test_percolator_2']}"
        );

        MatchParser matchParser = new MatchParser();
        MatchAssertion matchAssertion = matchParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("matches"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(List.class));
        List strings = (List) matchAssertion.getExpectedValue();
        assertThat(strings.size(), equalTo(2));
        assertThat(strings.get(0).toString(), equalTo("test_percolator_1"));
        assertThat(strings.get(1).toString(), equalTo("test_percolator_2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseMatchSourceValues() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "{ _source: { responses.0.hits.total: 3, foo: bar  }}"
        );

        MatchParser matchParser = new MatchParser();
        MatchAssertion matchAssertion = matchParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("_source"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Map.class));
        Map<String, Object> expectedValue = (Map<String, Object>) matchAssertion.getExpectedValue();
        assertThat(expectedValue.size(), equalTo(2));
        Object o = expectedValue.get("responses.0.hits.total");
        assertThat(o, instanceOf(Integer.class));
        assertThat((Integer)o, equalTo(3));
        o = expectedValue.get("foo");
        assertThat(o, instanceOf(String.class));
        assertThat(o.toString(), equalTo("bar"));
    }
}
