/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClientYamlTestSectionTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testWrongIndentation() throws Exception {
        {
            XContentParser parser = createParser(YamlXContent.yamlXContent, """
                "First test section":\s
                  - skip:
                    version:  "2.0.0 - 2.2.0"
                    reason:   "Update doesn't return metadata fields, waiting for #3259\"""");

            ParsingException e = expectThrows(ParsingException.class, () -> ClientYamlTestSection.parse(parser));
            assertEquals("Error parsing test named [First test section]", e.getMessage());
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals(
                "Expected [START_OBJECT, found [VALUE_NULL], the skip section is not properly indented",
                e.getCause().getMessage()
            );
        }
        {
            XContentParser parser = createParser(YamlXContent.yamlXContent, """
                "First test section":\s
                 - do :
                   catch: missing
                   indices.get_warmer:
                       index: test_index
                       name: test_warmer""");
            ParsingException e = expectThrows(ParsingException.class, () -> ClientYamlTestSection.parse(parser));
            assertEquals("Error parsing test named [First test section]", e.getMessage());
            assertThat(e.getCause(), instanceOf(IOException.class));
            assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals(
                "expected [START_OBJECT], found [VALUE_NULL], the do section is not properly indented",
                e.getCause().getCause().getMessage()
            );
        }
    }

    public void testParseTestSectionWithDoSection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "First test section":\s
             - do :
                 catch: missing
                 indices.get_warmer:
                     index: test_index
                     name: test_warmer""");

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("First test section"));
        assertThat(testSection.getSkipSection(), equalTo(SkipSection.EMPTY));
        assertThat(testSection.getExecutableSections().size(), equalTo(1));
        DoSection doSection = (DoSection) testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_warmer"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
    }

    public void testParseTestSectionWithDoSetAndSkipSectionsNoSkip() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "First test section":\s
              - skip:
                  version:  "6.0.0 - 6.2.0"
                  reason:   "Update doesn't return metadata fields, waiting for #3259"
              - do :
                  catch: missing
                  indices.get_warmer:
                      index: test_index
                      name: test_warmer
              - set: {_scroll_id: scroll_id}""");

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("First test section"));
        assertThat(testSection.getSkipSection(), notNullValue());
        assertThat(testSection.getSkipSection().getLowerVersion(), equalTo(Version.fromString("6.0.0")));
        assertThat(testSection.getSkipSection().getUpperVersion(), equalTo(Version.fromString("6.2.0")));
        assertThat(testSection.getSkipSection().getReason(), equalTo("Update doesn't return metadata fields, waiting for #3259"));
        assertThat(testSection.getExecutableSections().size(), equalTo(2));
        DoSection doSection = (DoSection) testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_warmer"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        SetSection setSection = (SetSection) testSection.getExecutableSections().get(1);
        assertThat(setSection.getStash().size(), equalTo(1));
        assertThat(setSection.getStash().get("_scroll_id"), equalTo("scroll_id"));
    }

    public void testParseTestSectionWithMultipleDoSections() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "Basic":

              - do:
                  index:
                    index: test_1
                    type:  test
                    id:    中文
                    body:  { "foo": "Hello: 中文" }
              - do:
                  get:
                    index: test_1
                    type:  test
                    id:    中文""");

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("Basic"));
        assertThat(testSection.getSkipSection(), equalTo(SkipSection.EMPTY));
        assertThat(testSection.getExecutableSections().size(), equalTo(2));
        DoSection doSection = (DoSection) testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("index"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        doSection = (DoSection) testSection.getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
    }

    public void testParseTestSectionWithDoSectionsAndAssertions() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "Basic":

              - do:
                  index:
                    index: test_1
                    type:  test
                    id:    中文
                    body:  { "foo": "Hello: 中文" }

              - do:
                  get:
                    index: test_1
                    type:  test
                    id:    中文

              - match: { _index:   test_1 }
              - is_true: _source
              - match: { _source:  { foo: "Hello: 中文" } }

              - do:
                  get:
                    index: test_1
                    id:    中文

              - length: { _index:   6 }
              - is_false: whatever
              - gt: { size: 5      }
              - lt: { size: 10      }""");

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("Basic"));
        assertThat(testSection.getSkipSection(), equalTo(SkipSection.EMPTY));
        assertThat(testSection.getExecutableSections().size(), equalTo(10));

        DoSection doSection = (DoSection) testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("index"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));

        doSection = (DoSection) testSection.getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));

        MatchAssertion matchAssertion = (MatchAssertion) testSection.getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("_index"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("test_1"));

        IsTrueAssertion trueAssertion = (IsTrueAssertion) testSection.getExecutableSections().get(3);
        assertThat(trueAssertion.getField(), equalTo("_source"));

        matchAssertion = (MatchAssertion) testSection.getExecutableSections().get(4);
        assertThat(matchAssertion.getField(), equalTo("_source"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Map.class));
        Map<?, ?> map = (Map<?, ?>) matchAssertion.getExpectedValue();
        assertThat(map.size(), equalTo(1));
        assertThat(map.get("foo").toString(), equalTo("Hello: 中文"));

        doSection = (DoSection) testSection.getExecutableSections().get(5);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));

        LengthAssertion lengthAssertion = (LengthAssertion) testSection.getExecutableSections().get(6);
        assertThat(lengthAssertion.getField(), equalTo("_index"));
        assertThat(lengthAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat(lengthAssertion.getExpectedValue(), equalTo(6));

        IsFalseAssertion falseAssertion = (IsFalseAssertion) testSection.getExecutableSections().get(7);
        assertThat(falseAssertion.getField(), equalTo("whatever"));

        GreaterThanAssertion greaterThanAssertion = (GreaterThanAssertion) testSection.getExecutableSections().get(8);
        assertThat(greaterThanAssertion.getField(), equalTo("size"));
        assertThat(greaterThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat(greaterThanAssertion.getExpectedValue(), equalTo(5));

        LessThanAssertion lessThanAssertion = (LessThanAssertion) testSection.getExecutableSections().get(9);
        assertThat(lessThanAssertion.getField(), equalTo("size"));
        assertThat(lessThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat(lessThanAssertion.getExpectedValue(), equalTo(10));
    }

    public void testSmallSection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "node_info test":
              - do:
                  cluster.node_info: {}
             \s
              - is_true: nodes
              - is_true: cluster_name
            """);

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);
        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("node_info test"));
        assertThat(testSection.getExecutableSections().size(), equalTo(3));
    }
}
