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

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseCompiledYamlTestCase extends BaseESClientYamlSuiteTestCase {

    private final ClientYamlTestSuite suite;
    private final Map<String, ClientYamlTestSection> mapping;

    protected BaseCompiledYamlTestCase() throws IOException {
        suite = createTestSuite();
        mapping = suite.getTestSections().stream().collect(Collectors.toMap(ClientYamlTestSection::getName, s -> s));
    }

    protected ClientYamlTestSuite createTestSuite() throws IOException {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ExecutableSection.DEFAULT_EXECUTABLE_CONTEXTS.size() + 1);
        entries.addAll(ExecutableSection.DEFAULT_EXECUTABLE_CONTEXTS);
        entries.add(new NamedXContentRegistry.Entry(ExecutableSection.class,
            new ParseField("compare_analyzers"), getParser()));
        NamedXContentRegistry executableSectionRegistry = new NamedXContentRegistry(entries);
        try (XContentParser parser = YamlXContent.yamlXContent.createParser(executableSectionRegistry,
            LoggingDeprecationHandler.INSTANCE, getYaml())) {
            return ClientYamlTestSuite.parse("", "", parser);
        } catch (Exception e) {
            throw new IOException("", e);
        }
    }

    protected CheckedFunction<XContentParser, ExecutableSection, IOException> getParser() {
        return ExecutableSection::parse;
    }

    protected String getYaml(){
        return Arrays.stream(getClass().getDeclaredClasses()).filter(cl->cl.getSimpleName().startsWith("BodyPart")).map(cl-> {
            try {
                return cl.getMethod("getYaml").invoke(null);
            } catch (Exception e) {
                throw new RuntimeException(e);}
        }).map(String.class::cast).collect(Collectors.joining());
    }

    protected void runTest(String testName) throws IOException {
        ClientYamlTestSection testSection = mapping.get(testName);
        runTest(new ClientYamlTestCandidate(suite, testSection));
    }
}

