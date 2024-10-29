/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClientYamlTestSuiteTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testParseTestSetupWithSkip() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            ---
            setup:
              - skip:
                  known_issues:
                    - cluster_feature: "feature_a"
                      fixed_by: "feature_a_fix"
                  reason: "Bug introduced with feature a, fixed with feature a fix"

            ---
            date:
              - skip:
                  cluster_features: "tsdb_indexing"
                  reason: tsdb indexing changed in 8.2.0
              - do:
                  indices.get_mapping:
                    index: test_index

              - match: {test_index.test_type.properties.text.type:     string}
              - match: {test_index.test_type.properties.text.analyzer: whitespace}
            """);

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser);

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo(getTestName()));
        assertThat(restTestSuite.getFile().isPresent(), equalTo(false));
        assertThat(restTestSuite.getSetupSection(), notNullValue());

        assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(false));
        assertThat(restTestSuite.getSetupSection().getPrerequisiteSection().isEmpty(), equalTo(false));
        assertThat(restTestSuite.getSetupSection().getExecutableSections().isEmpty(), equalTo(true));

        assertThat(restTestSuite.getTestSections().size(), equalTo(1));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("date"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(false));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(3));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(1));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
    }

    public void testParseTestSetupTeardownAndSections() throws Exception {
        final boolean includeSetup = randomBoolean();
        final boolean includeTeardown = randomBoolean();
        StringBuilder testSpecBuilder = new StringBuilder();
        if (includeSetup) {
            testSpecBuilder.append("""
                ---
                setup:
                  - do:
                        indices.create:
                          index: test_index

                """);
        }
        if (includeTeardown) {
            testSpecBuilder.append("""
                ---
                teardown:
                  - do:
                      indices.delete:
                        index: test_index

                """);
        }
        parser = createParser(YamlXContent.yamlXContent, testSpecBuilder + """
            ---
            "Get index mapping":
              - do:
                  indices.get_mapping:
                    index: test_index

              - match: {test_index.test_type.properties.text.type:     string}
              - match: {test_index.test_type.properties.text.analyzer: whitespace}

            ---
            "Get type mapping - pre 6.0":

              - skip:
                  cluster_features: "feature_in_6.0"
                  reason:      "for newer versions the index name is always returned"

              - do:
                  indices.get_mapping:
                    index: test_index
                    type: test_type

              - match: {test_type.properties.text.type:     string}
              - match: {test_type.properties.text.analyzer: whitespace}
            """);

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser);

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo(getTestName()));
        assertThat(restTestSuite.getFile().isPresent(), equalTo(false));
        assertThat(restTestSuite.getSetupSection(), notNullValue());
        if (includeSetup) {
            assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(false));
            assertThat(restTestSuite.getSetupSection().getPrerequisiteSection().isEmpty(), equalTo(true));
            assertThat(restTestSuite.getSetupSection().getExecutableSections().size(), equalTo(1));
            final ExecutableSection maybeDoSection = restTestSuite.getSetupSection().getExecutableSections().get(0);
            assertThat(maybeDoSection, instanceOf(DoSection.class));
            final DoSection doSection = (DoSection) maybeDoSection;
            assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.create"));
            assertThat(doSection.getApiCallSection().getParams().size(), equalTo(1));
            assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        } else {
            assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(true));
        }

        assertThat(restTestSuite.getTeardownSection(), notNullValue());
        if (includeTeardown) {
            assertThat(restTestSuite.getTeardownSection().isEmpty(), equalTo(false));
            assertThat(restTestSuite.getTeardownSection().getPrerequisiteSection().isEmpty(), equalTo(true));
            assertThat(restTestSuite.getTeardownSection().getDoSections().size(), equalTo(1));
            assertThat(
                ((DoSection) restTestSuite.getTeardownSection().getDoSections().get(0)).getApiCallSection().getApi(),
                equalTo("indices.delete")
            );
            assertThat(
                ((DoSection) restTestSuite.getTeardownSection().getDoSections().get(0)).getApiCallSection().getParams().size(),
                equalTo(1)
            );
            assertThat(
                ((DoSection) restTestSuite.getTeardownSection().getDoSections().get(0)).getApiCallSection().getParams().get("index"),
                equalTo("test_index")
            );
        } else {
            assertThat(restTestSuite.getTeardownSection().isEmpty(), equalTo(true));
        }

        assertThat(restTestSuite.getTestSections().size(), equalTo(2));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Get index mapping"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(3));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(1));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(MatchAssertion.class));
        MatchAssertion matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(1);
        assertThat(matchAssertion.getField(), equalTo("test_index.test_type.properties.text.type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("string"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(2), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("test_index.test_type.properties.text.analyzer"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("whitespace"));

        assertThat(restTestSuite.getTestSections().get(1).getName(), equalTo("Get type mapping - pre 6.0"));
        assertThat(restTestSuite.getTestSections().get(1).getPrerequisiteSection().isEmpty(), equalTo(false));
        assertThat(
            restTestSuite.getTestSections().get(1).getPrerequisiteSection().skipReason,
            equalTo("for newer versions the index name is always returned")
        );

        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().size(), equalTo(3));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(0), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(1).getExecutableSections().get(0);
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        assertThat(doSection.getApiCallSection().getParams().get("type"), equalTo("test_type"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(1).getExecutableSections().get(1);
        assertThat(matchAssertion.getField(), equalTo("test_type.properties.text.type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("string"));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(2), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(1).getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("test_type.properties.text.analyzer"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("whitespace"));
    }

    public void testParseTestSingleTestSection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            ---
            "Index with ID":

             - do:
                  index:
                      index:  test-weird-index-中文
                      type:   weird.type
                      id:     1
                      body:   { foo: bar }

             - is_true:   ok
             - match:   { _index:   test-weird-index-中文 }
             - match:   { _type:    weird.type }
             - match:   { _id:      "1"}
             - match:   { _version: 1}

             - do:
                  get:
                      index:  test-weird-index-中文
                      type:   weird.type
                      id:     1

             - match:   { _index:   test-weird-index-中文 }
             - match:   { _type:    weird.type }
             - match:   { _id:      "1"}
             - match:   { _version: 1}
             - match:   { _source: { foo: bar }}""");

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser);

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo(getTestName()));
        assertThat(restTestSuite.getFile().isPresent(), equalTo(false));

        assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(true));

        assertThat(restTestSuite.getTestSections().size(), equalTo(1));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Index with ID"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(12));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("index"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(IsTrueAssertion.class));
        IsTrueAssertion trueAssertion = (IsTrueAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(1);
        assertThat(trueAssertion.getField(), equalTo("ok"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(2), instanceOf(MatchAssertion.class));
        MatchAssertion matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("_index"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("test-weird-index-中文"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(3), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(3);
        assertThat(matchAssertion.getField(), equalTo("_type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("weird.type"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(4), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(4);
        assertThat(matchAssertion.getField(), equalTo("_id"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(5), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(5);
        assertThat(matchAssertion.getField(), equalTo("_version"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(6), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(6);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(7), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(7);
        assertThat(matchAssertion.getField(), equalTo("_index"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("test-weird-index-中文"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(8), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(8);
        assertThat(matchAssertion.getField(), equalTo("_type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("weird.type"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(9), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(9);
        assertThat(matchAssertion.getField(), equalTo("_id"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(10), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(10);
        assertThat(matchAssertion.getField(), equalTo("_version"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(11), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(11);
        assertThat(matchAssertion.getField(), equalTo("_source"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Map.class));
        assertThat(((Map) matchAssertion.getExpectedValue()).get("foo").toString(), equalTo("bar"));
    }

    public void testParseTestMultipleTestSections() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            ---
            "Missing document (partial doc)":

              - do:
                  catch:      missing
                  update:
                      index:  test_1
                      type:   test
                      id:     1
                      body:   { doc: { foo: bar } }

              - do:
                  update:
                      index: test_1
                      type:  test
                      id:    1
                      body:  { doc: { foo: bar } }
                      ignore: 404

            ---
            "Missing document (script)":


              - do:
                  catch:      missing
                  update:
                      index:  test_1
                      type:   test
                      id:     1
                      body:
                        script: "ctx._source.foo = bar"
                        params: { bar: 'xxx' }

              - do:
                  update:
                      index:  test_1
                      type:   test
                      id:     1
                      ignore: 404
                      body:
                        script:       "ctx._source.foo = bar"
                        params:       { bar: 'xxx' }
            """);

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser);

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo(getTestName()));
        assertThat(restTestSuite.getFile().isPresent(), equalTo(false));

        assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(true));

        assertThat(restTestSuite.getTestSections().size(), equalTo(2));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Missing document (partial doc)"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(2));

        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(4));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));

        assertThat(restTestSuite.getTestSections().get(1).getName(), equalTo("Missing document (script)"));
        assertThat(restTestSuite.getTestSections().get(1).getPrerequisiteSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().size(), equalTo(2));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(0), instanceOf(DoSection.class));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(1), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(1).getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(1).getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(4));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
    }

    public void testParseTestDuplicateTestSections() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            ---
            "Missing document (script)":

              - do:
                  catch:      missing
                  update:
                      index:  test_1
                      type:   test
                      id:     1
                      body:   { doc: { foo: bar } }

            ---
            "Missing document (script)":


              - do:
                  catch:      missing
                  update:
                      index:  test_1
                      type:   test
                      id:     1
                      body:
                        script: "ctx._source.foo = bar"
                        params: { bar: 'xxx' }

            """);

        Exception e = expectThrows(
            ParsingException.class,
            () -> ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser)
        );
        assertThat(e.getMessage(), containsString("duplicate test section"));
    }

    public void testParseSkipOs() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "Broken on some os":

              - skip:
                  features:     skip_os
                  os:           ["windows95", "debian-5"]
                  reason:      "not supported"

              - do:
                  indices.get_mapping:
                    index: test_index
                    type: test_type

              - match: {test_type.properties.text.type:     string}
              - match: {test_type.properties.text.analyzer: whitespace}
            """);

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser);

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo(getTestName()));
        assertThat(restTestSuite.getFile().isPresent(), equalTo(false));
        assertThat(restTestSuite.getTestSections().size(), equalTo(1));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Broken on some os"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(false));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().skipReason, containsString("not supported"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().hasYamlRunnerFeature("skip_os"), equalTo(true));
    }

    public void testMuteUsingAwaitsFix() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "Mute":

              - skip:
                  awaits_fix: bugurl

              - do:
                  indices.get_mapping:
                    index: test_index
                    type: test_type

              - match: {test_type.properties.text.type:     string}
              - match: {test_type.properties.text.analyzer: whitespace}
            """);

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser);

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo(getTestName()));
        assertThat(restTestSuite.getFile().isPresent(), equalTo(false));
        assertThat(restTestSuite.getTestSections().size(), equalTo(1));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Mute"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(false));
    }

    public void testParseSkipAndRequireClusterFeatures() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, """
            "Broken on some os":

              - skip:
                  known_issues:
                    - cluster_feature: buggy_feature
                      fixed_by: buggy_feature_fix
                  cluster_features:     [unsupported-feature1, unsupported-feature2]
                  reason:      "unsupported-features are not supported"
              - requires:
                  cluster_features:     required-feature1
                  reason:      "required-feature1 is required"
              - do:
                  indices.get_mapping:
                    index: test_index
                    type: test_type

              - match: {test_type.properties.text.type:     string}
              - match: {test_type.properties.text.analyzer: whitespace}
            """);

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(getTestClass().getName(), getTestName(), Optional.empty(), parser);

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo(getTestName()));
        assertThat(restTestSuite.getFile().isPresent(), equalTo(false));
        assertThat(restTestSuite.getTestSections().size(), equalTo(1));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Broken on some os"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(false));
        assertThat(
            restTestSuite.getTestSections().get(0).getPrerequisiteSection().skipReason,
            equalTo("unsupported-features are not supported")
        );
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().requireReason, equalTo("required-feature1 is required"));
    }

    public void testParseFileWithSingleTestSection() throws Exception {
        final Path filePath = createTempFile("tyf", ".yml");
        Files.writeString(filePath, """
            ---
            "Index with ID":

             - do:
                  index:
                      index:  test-weird-index-中文
                      type:   weird.type
                      id:     1
                      body:   { foo: bar }

             - is_true:   ok""" + "\n");

        ClientYamlTestSuite restTestSuite = ClientYamlTestSuite.parse(ExecutableSection.XCONTENT_REGISTRY, "api", filePath);

        assertThat(restTestSuite, notNullValue());
        assertThat(
            restTestSuite.getName(),
            equalTo(filePath.getFileName().toString().substring(0, filePath.getFileName().toString().lastIndexOf('.')))
        );
        assertThat(restTestSuite.getFile().isPresent(), equalTo(true));
        assertThat(restTestSuite.getFile().get(), equalTo(filePath));

        assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(true));

        assertThat(restTestSuite.getTestSections().size(), equalTo(1));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Index with ID"));
        assertThat(restTestSuite.getTestSections().get(0).getPrerequisiteSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(2));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("index"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(IsTrueAssertion.class));
        IsTrueAssertion trueAssertion = (IsTrueAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(1);
        assertThat(trueAssertion.getField(), equalTo("ok"));
    }

    public void testAddingDoWithoutSkips() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setApiCallSection(new ApiCallSection("test"));
        ClientYamlTestSection section = new ClientYamlTestSection(
            new XContentLocation(0, 0),
            "test",
            PrerequisiteSection.EMPTY,
            Collections.singletonList(doSection)
        );
        ClientYamlTestSuite clientYamlTestSuite = new ClientYamlTestSuite(
            "api",
            "name",
            Optional.empty(),
            SetupSection.EMPTY,
            TeardownSection.EMPTY,
            Collections.singletonList(section)
        );
        clientYamlTestSuite.validate();
    }

    public void testAddingDoWithWarningWithoutSkipWarnings() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeaders(singletonList("foo"));
        doSection.setApiCallSection(new ApiCallSection("test"));
        ClientYamlTestSuite testSuite = createTestSuite(PrerequisiteSection.EMPTY, doSection);
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertThat(e.getMessage(), containsString(Strings.format("""
            api/name:
            attempted to add a [do] with a [warnings] section without a corresponding ["requires": "test_runner_features": "warnings"] \
            so runners that do not support the [warnings] section can skip the test at line [%d]\
            """, lineNumber)));
    }

    public void testAddingDoWithWarningRegexWithoutSkipWarnings() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeadersRegex(singletonList(Pattern.compile("foo")));
        doSection.setApiCallSection(new ApiCallSection("test"));
        ClientYamlTestSuite testSuite = createTestSuite(PrerequisiteSection.EMPTY, doSection);
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertThat(e.getMessage(), containsString(Strings.format("""
            api/name:
            attempted to add a [do] with a [warnings_regex] section without a corresponding \
            ["requires": "test_runner_features": "warnings_regex"] \
            so runners that do not support the [warnings_regex] section can skip the test at line [%d]\
            """, lineNumber)));
    }

    public void testAddingDoWithAllowedWarningWithoutSkipAllowedWarnings() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setAllowedWarningHeaders(singletonList("foo"));
        doSection.setApiCallSection(new ApiCallSection("test"));
        ClientYamlTestSuite testSuite = createTestSuite(PrerequisiteSection.EMPTY, doSection);
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertThat(e.getMessage(), containsString(Strings.format("""
            api/name:
            attempted to add a [do] with a [allowed_warnings] section without a corresponding ["requires": "test_runner_features": \
            "allowed_warnings"] so runners that do not support the [allowed_warnings] section can skip the test at \
            line [%d]\
            """, lineNumber)));
    }

    public void testAddingDoWithAllowedWarningRegexWithoutSkipAllowedWarnings() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setAllowedWarningHeadersRegex(singletonList(Pattern.compile("foo")));
        doSection.setApiCallSection(new ApiCallSection("test"));
        ClientYamlTestSuite testSuite = createTestSuite(PrerequisiteSection.EMPTY, doSection);
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertThat(e.getMessage(), containsString(Strings.format("""
            api/name:
            attempted to add a [do] with a [allowed_warnings_regex] section without a corresponding ["requires": "test_runner_features": \
            "allowed_warnings_regex"] so runners that do not support the [allowed_warnings_regex] section can skip the test \
            at line [%d]\
            """, lineNumber)));
    }

    public void testAddingDoWithHeaderWithoutSkipHeaders() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        ApiCallSection apiCallSection = new ApiCallSection("test");
        apiCallSection.addHeaders(Collections.singletonMap("header", "value"));
        doSection.setApiCallSection(apiCallSection);
        ClientYamlTestSuite testSuite = createTestSuite(PrerequisiteSection.EMPTY, doSection);
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertThat(e.getMessage(), containsString(Strings.format("""
            api/name:
            attempted to add a [do] with a [headers] section without a corresponding ["requires": "test_runner_features": "headers"] \
            so runners that do not support the [headers] section can skip the test at line [%d]\
            """, lineNumber)));
    }

    public void testAddingDoWithNodeSelectorWithoutSkipNodeSelector() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        ApiCallSection apiCall = new ApiCallSection("test");
        apiCall.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        doSection.setApiCallSection(apiCall);
        ClientYamlTestSuite testSuite = createTestSuite(PrerequisiteSection.EMPTY, doSection);
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertThat(e.getMessage(), containsString(Strings.format("""
            api/name:
            attempted to add a [do] with a [node_selector] section without a corresponding \
            ["requires": "test_runner_features": "node_selector"] \
            so runners that do not support the [node_selector] section can skip the test at line [%d]\
            """, lineNumber)));
    }

    public void testAddingContainsWithoutSkipContains() {
        int lineNumber = between(1, 10000);
        ContainsAssertion containsAssertion = new ContainsAssertion(
            new XContentLocation(lineNumber, 0),
            randomAlphaOfLength(randomIntBetween(3, 30)),
            randomDouble()
        );
        ClientYamlTestSuite testSuite = createTestSuite(PrerequisiteSection.EMPTY, containsAssertion);
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertThat(e.getMessage(), containsString(Strings.format("""
            api/name:
            attempted to add a [contains] assertion without a corresponding ["requires": "test_runner_features": "contains"] \
            so runners that do not support the [contains] assertion can skip the test at line [%d]\
            """, lineNumber)));
    }

    public void testMultipleValidationErrors() {
        int firstLineNumber = between(1, 10000);
        List<ClientYamlTestSection> sections = new ArrayList<>();
        {
            ContainsAssertion containsAssertion = new ContainsAssertion(
                new XContentLocation(firstLineNumber, 0),
                randomAlphaOfLength(randomIntBetween(3, 30)),
                randomDouble()
            );
            sections.add(
                new ClientYamlTestSection(
                    new XContentLocation(0, 0),
                    "section1",
                    PrerequisiteSection.EMPTY,
                    Collections.singletonList(containsAssertion)
                )
            );
        }
        int secondLineNumber = between(1, 10000);
        int thirdLineNumber = between(1, 10000);
        List<ExecutableSection> doSections = new ArrayList<>();
        {
            DoSection doSection = new DoSection(new XContentLocation(secondLineNumber, 0));
            doSection.setExpectedWarningHeaders(singletonList("foo"));
            doSection.setApiCallSection(new ApiCallSection("test"));
            doSections.add(doSection);
        }
        {
            DoSection doSection = new DoSection(new XContentLocation(thirdLineNumber, 0));
            ApiCallSection apiCall = new ApiCallSection("test");
            apiCall.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            doSection.setApiCallSection(apiCall);
            doSections.add(doSection);
        }
        sections.add(new ClientYamlTestSection(new XContentLocation(0, 0), "section2", PrerequisiteSection.EMPTY, doSections));

        ClientYamlTestSuite testSuite = new ClientYamlTestSuite(
            "api",
            "name",
            Optional.empty(),
            SetupSection.EMPTY,
            TeardownSection.EMPTY,
            sections
        );
        Exception e = expectThrows(IllegalArgumentException.class, testSuite::validate);
        assertEquals(Strings.format("""
            api/name:
            attempted to add a [contains] assertion without a corresponding \
            ["requires": "test_runner_features": "contains"] \
            so runners that do not support the [contains] assertion can skip the test at line [%d],
            attempted to add a [do] with a [warnings] section without a corresponding \
            ["requires": "test_runner_features": "warnings"] \
            so runners that do not support the [warnings] section can skip the test at line [%d],
            attempted to add a [do] with a [node_selector] section without a corresponding \
            ["requires": "test_runner_features": "node_selector"] \
            so runners that do not support the [node_selector] section can skip the test at line [%d]\
            """, firstLineNumber, secondLineNumber, thirdLineNumber), e.getMessage());
    }

    private static PrerequisiteSection createPrerequisiteSection(String yamlTestRunnerFeature) {
        return new PrerequisiteSection(emptyList(), null, emptyList(), null, singletonList(yamlTestRunnerFeature));
    }

    public void testAddingDoWithWarningWithSkip() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeaders(singletonList("foo"));
        doSection.setApiCallSection(new ApiCallSection("test"));
        PrerequisiteSection prerequisiteSection = createPrerequisiteSection("warnings");
        createTestSuite(prerequisiteSection, doSection).validate();
    }

    public void testAddingDoWithWarningRegexWithSkip() {
        int lineNumber = between(1, 10000);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeadersRegex(singletonList(Pattern.compile("foo")));
        doSection.setApiCallSection(new ApiCallSection("test"));
        PrerequisiteSection prerequisiteSection = createPrerequisiteSection("warnings_regex");
        createTestSuite(prerequisiteSection, doSection).validate();
    }

    public void testAddingDoWithNodeSelectorWithSkip() {
        int lineNumber = between(1, 10000);
        PrerequisiteSection prerequisiteSection = createPrerequisiteSection("node_selector");
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        ApiCallSection apiCall = new ApiCallSection("test");
        apiCall.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        doSection.setApiCallSection(apiCall);
        createTestSuite(prerequisiteSection, doSection).validate();
    }

    public void testAddingDoWithHeadersWithSkip() {
        int lineNumber = between(1, 10000);
        PrerequisiteSection prerequisiteSection = createPrerequisiteSection("headers");
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        ApiCallSection apiCallSection = new ApiCallSection("test");
        apiCallSection.addHeaders(singletonMap("foo", "bar"));
        doSection.setApiCallSection(apiCallSection);
        createTestSuite(prerequisiteSection, doSection).validate();
    }

    public void testAddingContainsWithSkip() {
        int lineNumber = between(1, 10000);
        PrerequisiteSection prerequisiteSection = createPrerequisiteSection("contains");
        ContainsAssertion containsAssertion = new ContainsAssertion(
            new XContentLocation(lineNumber, 0),
            randomAlphaOfLength(randomIntBetween(3, 30)),
            randomDouble()
        );
        createTestSuite(prerequisiteSection, containsAssertion).validate();
    }

    public void testAddingCloseToWithSkip() {
        int lineNumber = between(1, 10000);
        PrerequisiteSection prerequisiteSection = createPrerequisiteSection("close_to");
        CloseToAssertion closeToAssertion = new CloseToAssertion(
            new XContentLocation(lineNumber, 0),
            randomAlphaOfLength(randomIntBetween(3, 30)),
            randomDouble(),
            randomDouble()
        );
        createTestSuite(prerequisiteSection, closeToAssertion).validate();
    }

    public void testAddingIsAfterWithSkip() {
        int lineNumber = between(1, 10000);
        PrerequisiteSection prerequisiteSection = createPrerequisiteSection("is_after");
        IsAfterAssertion isAfterAssertion = new IsAfterAssertion(
            new XContentLocation(lineNumber, 0),
            randomAlphaOfLength(randomIntBetween(3, 30)),
            randomInstantBetween(Instant.ofEpochSecond(0L), Instant.ofEpochSecond(3000000000L))
        );
        createTestSuite(prerequisiteSection, isAfterAssertion).validate();
    }

    private static ClientYamlTestSuite createTestSuite(PrerequisiteSection prerequisiteSection, ExecutableSection executableSection) {
        final SetupSection setupSection;
        final TeardownSection teardownSection;
        final ClientYamlTestSection clientYamlTestSection;
        switch (randomIntBetween(0, 4)) {
            case 0 -> {
                setupSection = new SetupSection(prerequisiteSection, Collections.emptyList());
                teardownSection = TeardownSection.EMPTY;
                clientYamlTestSection = new ClientYamlTestSection(
                    new XContentLocation(0, 0),
                    "test",
                    PrerequisiteSection.EMPTY,
                    Collections.singletonList(executableSection)
                );
            }
            case 1 -> {
                setupSection = SetupSection.EMPTY;
                teardownSection = new TeardownSection(prerequisiteSection, Collections.emptyList());
                clientYamlTestSection = new ClientYamlTestSection(
                    new XContentLocation(0, 0),
                    "test",
                    PrerequisiteSection.EMPTY,
                    Collections.singletonList(executableSection)
                );
            }
            case 2 -> {
                setupSection = SetupSection.EMPTY;
                teardownSection = TeardownSection.EMPTY;
                clientYamlTestSection = new ClientYamlTestSection(
                    new XContentLocation(0, 0),
                    "test",
                    prerequisiteSection,
                    Collections.singletonList(executableSection)
                );
            }
            case 3 -> {
                setupSection = new SetupSection(prerequisiteSection, Collections.singletonList(executableSection));
                teardownSection = TeardownSection.EMPTY;
                clientYamlTestSection = new ClientYamlTestSection(
                    new XContentLocation(0, 0),
                    "test",
                    PrerequisiteSection.EMPTY,
                    randomBoolean() ? Collections.emptyList() : Collections.singletonList(executableSection)
                );
            }
            case 4 -> {
                setupSection = SetupSection.EMPTY;
                teardownSection = new TeardownSection(prerequisiteSection, Collections.singletonList(executableSection));
                clientYamlTestSection = new ClientYamlTestSection(
                    new XContentLocation(0, 0),
                    "test",
                    PrerequisiteSection.EMPTY,
                    randomBoolean() ? Collections.emptyList() : Collections.singletonList(executableSection)
                );
            }
            default -> throw new UnsupportedOperationException();
        }
        return new ClientYamlTestSuite(
            "api",
            "name",
            Optional.empty(),
            setupSection,
            teardownSection,
            Collections.singletonList(clientYamlTestSection)
        );
    }
}
