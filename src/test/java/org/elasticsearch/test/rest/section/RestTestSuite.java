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
package org.elasticsearch.test.rest.section;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Holds a REST test suite loaded from a specific yaml file.
 * Supports a setup section and multiple test sections.
 */
public class RestTestSuite {

    private final String api;
    private final String name;

    private SetupSection setupSection;

    private Set<TestSection> testSections = Sets.newTreeSet();

    public RestTestSuite(String api, String name) {
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

    /**
     * Adds a {@link org.elasticsearch.test.rest.section.TestSection} to the REST suite
     * @return true if the test section was not already present, false otherwise
     */
    public boolean addTestSection(TestSection testSection) {
        return this.testSections.add(testSection);
    }

    public List<TestSection> getTestSections() {
        return Lists.newArrayList(testSections);
    }
}
