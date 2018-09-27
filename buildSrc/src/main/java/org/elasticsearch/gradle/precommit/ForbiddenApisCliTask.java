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
package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.JavaVersion;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.JavaExecSpec;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ForbiddenApisCliTask extends PrecommitTask {

    private final Logger logger = Logging.getLogger(ForbiddenApisCliTask.class);
    private FileCollection signaturesFiles;
    private List<String> signatures = new ArrayList<>();
    private Set<String> bundledSignatures = new LinkedHashSet<>();
    private Set<String> suppressAnnotations = new LinkedHashSet<>();
    private JavaVersion targetCompatibility;
    private FileCollection classesDirs;
    private SourceSet sourceSet;
    // This needs to be an object so it can hold Groovy GStrings
    private Object javaHome;

    @Input
    public JavaVersion getTargetCompatibility() {
        return targetCompatibility;
    }

    public void setTargetCompatibility(JavaVersion targetCompatibility) {
        if (targetCompatibility.compareTo(JavaVersion.VERSION_1_10) > 0) {
            logger.warn(
                "Target compatibility is set to {} but forbiddenapis only supports up to 10. Will cap at 10.",
                targetCompatibility
            );
            this.targetCompatibility = JavaVersion.VERSION_1_10;
        } else {
            this.targetCompatibility = targetCompatibility;
        }
    }

    @InputFiles
    @SkipWhenEmpty
    public FileCollection getClassesDirs() {
        return classesDirs.filter(File::exists);
    }

    public void setClassesDirs(FileCollection classesDirs) {
        this.classesDirs = classesDirs;
    }

    @InputFiles
    public FileCollection getSignaturesFiles() {
        return signaturesFiles;
    }

    public void setSignaturesFiles(FileCollection signaturesFiles) {
        this.signaturesFiles = signaturesFiles;
    }

    @Input
    public List<String> getSignatures() {
        return signatures;
    }

    public void setSignatures(List<String> signatures) {
        this.signatures = signatures;
    }

    @Input
    public Set<String> getBundledSignatures() {
        return bundledSignatures;
    }

    public void setBundledSignatures(Set<String> bundledSignatures) {
        this.bundledSignatures = bundledSignatures;
    }

    @Input
    public Set<String> getSuppressAnnotations() {
        return suppressAnnotations;
    }

    public void setSuppressAnnotations(Set<String> suppressAnnotations) {
        this.suppressAnnotations = suppressAnnotations;
    }

    @InputFiles
    public FileCollection getClassPathFromSourceSet() {
        return getProject().files(
            sourceSet.getCompileClasspath(),
            sourceSet.getRuntimeClasspath()
        );
    }

    public void setSourceSet(SourceSet sourceSet) {
        this.sourceSet = sourceSet;
    }

    @InputFiles
    public Configuration getForbiddenAPIsConfiguration() {
        return getProject().getConfigurations().getByName("forbiddenApisCliJar");
    }

    @Input
    public Object getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(Object javaHome) {
        this.javaHome = javaHome;
    }

    @TaskAction
    public void runForbiddenApisAndWriteMarker() {
        LoggedExec.javaexec(getProject(), (JavaExecSpec spec) -> {
            spec.classpath(
                getForbiddenAPIsConfiguration(),
                getClassPathFromSourceSet()
            );
            spec.setExecutable(getJavaHome() + "/bin/java");
            spec.setMain("de.thetaphi.forbiddenapis.cli.CliMain");
            // build the command line
            getSignaturesFiles().forEach(file -> spec.args("-f", file.getAbsolutePath()));
            getSuppressAnnotations().forEach(annotation -> spec.args("--suppressannotation", annotation));
            getBundledSignatures().forEach(bundled -> {
                    // there's no option for target compatibility so we have to interpret it
                    final String prefix;
                    if (bundled.equals("jdk-system-out") ||
                        bundled.equals("jdk-reflection") ||
                        bundled.equals("jdk-non-portable")) {
                        prefix = "";
                    } else {
                        prefix = "-" + (
                            getTargetCompatibility().compareTo(JavaVersion.VERSION_1_9) >= 0 ?
                                getTargetCompatibility().getMajorVersion() :
                                "1." + getTargetCompatibility().getMajorVersion())
                        ;
                    }
                    spec.args("-b", bundled + prefix);
                }
            );
            getClassesDirs().forEach(dir ->
                spec.args("-d", dir)
            );
        });
    }

}
