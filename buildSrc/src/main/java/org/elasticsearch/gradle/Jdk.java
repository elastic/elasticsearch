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

package org.elasticsearch.gradle;

import org.gradle.api.Buildable;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.TaskDependency;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class Jdk implements Buildable, Iterable<File> {

    static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)(\\.\\d+\\.\\d+)?\\+(\\d+)(@([a-f0-9]{32}))?");
    private static final List<String> ALLOWED_PLATFORMS = Collections.unmodifiableList(Arrays.asList("linux", "windows", "darwin"));

    private final String name;
    private final Configuration configuration;

    private final Property<String> version;
    private final Property<String> platform;


    Jdk(String name, Project project) {
        this.name = name;
        this.configuration = project.getConfigurations().create("jdk_" + name);
        this.version = project.getObjects().property(String.class);
        this.platform = project.getObjects().property(String.class);
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version.get();
    }

    public void setVersion(String version) {
        if (VERSION_PATTERN.matcher(version).matches() == false) {
            throw new IllegalArgumentException("malformed version [" + version + "] for jdk [" + name + "]");
        }
        this.version.set(version);
    }

    public String getPlatform() {
        return platform.get();
    }

    public void setPlatform(String platform) {
        if (ALLOWED_PLATFORMS.contains(platform) == false) {
            throw new IllegalArgumentException(
                "unknown platform [" + platform + "] for jdk [" + name + "], must be one of " + ALLOWED_PLATFORMS);
        }
        this.platform.set(platform);
    }

    // pkg private, for internal use
    Configuration getConfiguration() {
        return configuration;
    }

    public String getPath() {
        return configuration.getSingleFile().toString();
    }

    @Override
    public String toString() {
        return getPath();
    }

    @Override
    public TaskDependency getBuildDependencies() {
        return configuration.getBuildDependencies();
    }

    // internal, make this jdks configuration unmodifiable
    void finalizeValues() {
        if (version.isPresent() == false) {
            throw new IllegalArgumentException("version not specified for jdk [" + name + "]");
        }
        if (platform.isPresent() == false) {
            throw new IllegalArgumentException("platform not specified for jdk [" + name + "]");
        }
        version.finalizeValue();
        platform.finalizeValue();
    }

    @Override
    public Iterator<File> iterator() {
        return configuration.iterator();
    }
}
