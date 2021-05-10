/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.yaml.section.GreaterThanAssertion;
import org.elasticsearch.test.rest.yaml.section.IsFalseAssertion;
import org.elasticsearch.test.rest.yaml.section.IsTrueAssertion;
import org.elasticsearch.test.rest.yaml.section.LengthAssertion;
import org.elasticsearch.test.rest.yaml.section.LessThanAssertion;
import org.elasticsearch.test.rest.yaml.section.MatchAssertion;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class AssertionTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testParseIsTrue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "get.fields._timestamp"
        );

        IsTrueAssertion trueAssertion = IsTrueAssertion.parse(parser);

        assertThat(trueAssertion, notNullValue());
        assertThat(trueAssertion.getField(), equalTo("get.fields._timestamp"));
    }

    public void testParseIsFalse() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "docs.1._source"
        );

        IsFalseAssertion falseAssertion = IsFalseAssertion.parse(parser);

        assertThat(falseAssertion, notNullValue());
        assertThat(falseAssertion.getField(), equalTo("docs.1._source"));
    }

    public void testParseGreaterThan() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "{ field: 3}"
        );

        GreaterThanAssertion greaterThanAssertion = GreaterThanAssertion.parse(parser);
        assertThat(greaterThanAssertion, notNullValue());
        assertThat(greaterThanAssertion.getField(), equalTo("field"));
        assertThat(greaterThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) greaterThanAssertion.getExpectedValue(), equalTo(3));
    }

    public void testParseLessThan() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "{ field: 3}"
        );

        LessThanAssertion lessThanAssertion = LessThanAssertion.parse(parser);
        assertThat(lessThanAssertion, notNullValue());
        assertThat(lessThanAssertion.getField(), equalTo("field"));
        assertThat(lessThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lessThanAssertion.getExpectedValue(), equalTo(3));
    }

    public void testParseLength() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "{ _id: 22}"
        );

        LengthAssertion lengthAssertion = LengthAssertion.parse(parser);
        assertThat(lengthAssertion, notNullValue());
        assertThat(lengthAssertion.getField(), equalTo("_id"));
        assertThat(lengthAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lengthAssertion.getExpectedValue(), equalTo(22));
    }

    public void testParseMatchSimpleIntegerValue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "{ field: 10 }"
        );

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("field"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) matchAssertion.getExpectedValue(), equalTo(10));
    }

    public void testParseMatchSimpleStringValue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "{ foo: bar }"
        );

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("foo"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(String.class));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("bar"));
    }

    public void testParseMatchArray() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "{'matches': ['test_percolator_1', 'test_percolator_2']}"
        );

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("matches"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(List.class));
        List<?> strings = (List<?>) matchAssertion.getExpectedValue();
        assertThat(strings.size(), equalTo(2));
        assertThat(strings.get(0).toString(), equalTo("test_percolator_1"));
        assertThat(strings.get(1).toString(), equalTo("test_percolator_2"));
    }

    @SuppressWarnings("unchecked")
    public void testParseContains() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "{testKey: { someKey: someValue } }"
        );

        ContainsAssertion containsAssertion = ContainsAssertion.parse(parser);
        assertThat(containsAssertion, notNullValue());
        assertThat(containsAssertion.getField(), equalTo("testKey"));
        assertThat(containsAssertion.getExpectedValue(), instanceOf(Map.class));
        assertThat(
            ((Map<String, String>) containsAssertion.getExpectedValue()).get("someKey"),
            equalTo("someValue")
        );
    }

    @SuppressWarnings("unchecked")
    public void testParseMatchSourceValues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "{ _source: { responses.0.hits.total: 3, foo: bar  }}"
        );

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

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

    public void testCloseTo() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "{ field: { value: 42.2, error: 0.001 } }"
        );

        CloseToAssertion closeToAssertion = CloseToAssertion.parse(parser);

        assertThat(closeToAssertion, notNullValue());
        assertThat(closeToAssertion.getField(), equalTo("field"));
        assertThat(closeToAssertion.getExpectedValue(), instanceOf(Double.class));
        assertThat((Double) closeToAssertion.getExpectedValue(), equalTo(42.2));
        assertThat(closeToAssertion.getError(), equalTo(0.001));
        closeToAssertion.doAssert(42.2 + randomDoubleBetween(-0.001, 0.001, false), closeToAssertion.getExpectedValue());
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> closeToAssertion.doAssert(
                42.2 + (randomBoolean() ? 1 : -1) * randomDoubleBetween(0.001001, 10, false),
                closeToAssertion.getExpectedValue()
            )
        );
        assertThat(e.getMessage(), containsString("Expected: a numeric value within <0.001> of <42.2>"));
    }

    public void testInvalidCloseTo() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "{ field: 42 }"
        );
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->  CloseToAssertion.parse(parser));
        assertThat(exception.getMessage(), equalTo("expected a map with value and error but got Integer"));

        parser = createParser(YamlXContent.yamlXContent,
            "{ field: {  } }"
        );
        exception = expectThrows(IllegalArgumentException.class, () ->  CloseToAssertion.parse(parser));
        assertThat(exception.getMessage(), equalTo("expected a map with value and error but got a map with 0 fields"));

        parser = createParser(YamlXContent.yamlXContent,
            "{ field: { foo: 13, value: 15 } }"
        );
        exception = expectThrows(IllegalArgumentException.class, () ->  CloseToAssertion.parse(parser));
        assertThat(exception.getMessage(), equalTo("error is missing or not a number"));

        parser = createParser(YamlXContent.yamlXContent,
            "{ field: { foo: 13, bar: 15 } }"
        );
        exception = expectThrows(IllegalArgumentException.class, () ->  CloseToAssertion.parse(parser));
        assertThat(exception.getMessage(), equalTo("value is missing or not a number"));
    }
}
