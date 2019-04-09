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

import org.gradle.api.DefaultTask;
import org.elasticsearch.gradle.precommit.DependencyLicensesTask;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencyResolutionListener;
import org.gradle.api.artifacts.DependencySet;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A task to gather information about the dependencies and export them into a csv file.
 *
 * The following information is gathered:
 * <ul>
 *     <li>name: name that identifies the library (groupId:artifactId)</li>
 *     <li>version</li>
 *     <li>URL: link to have more information about the dependency.</li>
 *     <li>license: <a href="https://spdx.org/licenses/">SPDX license</a> identifier, custom license or UNKNOWN.</li>
 * </ul>
 *
 */
public class DependenciesInfoTask extends DefaultTask {
    
    public DependenciesInfoTask() {
        setDescription("Create a CSV file with dependencies information.");
    }
    
    /** Dependencies to gather information from. */
    private Configuration runtimeConfiguration;

    /** We subtract compile-only dependencies. */
    private Configuration compileOnlyConfiguration;
    
    private LinkedHashMap<String, String> mappings;

    /** Directory to read license files */
    private File licensesDir = new File(project.projectDir, "licenses");

    private File outputFile = new File(project.buildDir, "reports/dependencies/dependencies.csv");

    @Input
    public Configuration getRuntimeConfiguration() {
        return runtimeConfiguration;
    }

    public void setRuntimeConfiguration(Configuration runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    @Input
    public Configuration getCompileOnlyConfiguration() {
        return compileOnlyConfiguration;
    }

    public void setCompileOnlyConfiguration(Configuration compileOnlyConfiguration) {
        this.compileOnlyConfiguration = compileOnlyConfiguration;
    }

    @Input
    public LinkedHashMap<String, String> getMappings() {
        return mappings;
    }

    public void setMappings(LinkedHashMap<String, String> mappings) {
        this.mappings = mappings;
    }

    @InputDirectory
    public File getLicensesDir() {
        return licensesDir;
    }

    public void setLicensesDir(File licensesDir) {
        this.licensesDir = licensesDir;
    }

    @OutputFile
    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }
    
    

}
