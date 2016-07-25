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
package org.elasticsearch.test.rest.yaml.section;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Holds a REST test suite loaded from a specific yaml file.
 * Supports a setup section and multiple test sections.
 */
public class ClientYamlTestSuite {

    private final String api;
    private final String name;

    private SetupSection setupSection;
    private TeardownSection teardownSection;

    private Set<ClientYamlTestSection> testSections = new TreeSet<>();

    public ClientYamlTestSuite(String api, String name) {
        this.api = api;
        this.name = name;
    }

    public String getApi() {
        return api;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return api + "/" + name;
    }

    public SetupSection getSetupSection() {
        return setupSection;
    }

    public void setSetupSection(SetupSection setupSection) {
        this.setupSection = setupSection;
    }

    public TeardownSection getTeardownSection() {
        return teardownSection;
    }

    public void setTeardownSection(TeardownSection teardownSection) {
        this.teardownSection = teardownSection;
    }

    /**
     * Adds a {@link org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection} to the REST suite
     * @return true if the test section was not already present, false otherwise
     */
    public boolean addTestSection(ClientYamlTestSection testSection) {
        return this.testSections.add(testSection);
    }

    public List<ClientYamlTestSection> getTestSections() {
        return new ArrayList<>(testSections);
    }
}
