/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.section;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Represents a test section, which is composed of a skip section and multiple executable sections.
 */
public class TestSection {
    private final String name;
    private SkipSection skipSection;
    private final List<ExecutableSection> executableSections;

    public TestSection(String name) {
        this.name = name;
        this.executableSections = Lists.newArrayList();
    }

    public String getName() {
        return name;
    }

    public SkipSection getSkipSection() {
        return skipSection;
    }

    public void setSkipSection(SkipSection skipSection) {
        this.skipSection = skipSection;
    }

    public List<ExecutableSection> getExecutableSections() {
        return executableSections;
    }

    public void addExecutableSection(ExecutableSection executableSection) {
        this.executableSections.add(executableSection);
    }
}
