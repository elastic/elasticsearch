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
package org.elasticsearch.gradle.test.rest.report;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileTree;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.internal.Factory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Reports on all rest tests that are found in the committed source code as new line delimited file.
 */
public abstract class RestTestSourceReport extends RestResourceReport {

    private Set<String> includeProjects = new HashSet<>();
    private Set<PathMatcher> excludesMatchers = new HashSet<>();
    private static final String DOUBLE_STARS = "**"; // checkstyle thinks this is an empty javadoc statement, so string concat instead
    private static final String INCLUDE_PATTERN = DOUBLE_STARS + "/rest-api-spec/test/" + DOUBLE_STARS + "/*.yml";

    @Inject
    public RestTestSourceReport(String name, Task task) {
        super(name, task);
    }

    @Override
    void appendToReport(Project project, Logger logger) {
        if (includeProjects.contains(project.getPath())
            || includeProjects.stream() // check if current project is a child of an included project
                .anyMatch(
                    parent -> project.findProject(parent).getSubprojects().stream().anyMatch(sub -> sub.getPath().equals(project.getPath()))
                )) {
            try (FileWriter fileWriter = new FileWriter(this.getOutputLocation().getAsFile().get(), true)) {
                final Optional<FileTree> testFileTree = getTestSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
                final Optional<FileTree> mainFileTree = getMainSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
                FileTree sourceFileTree;
                if (testFileTree.isPresent() && mainFileTree.isPresent()) {
                    sourceFileTree = testFileTree.get().plus(mainFileTree.get());
                } else {
                    sourceFileTree = testFileTree.orElse(mainFileTree.orElse(null));
                }

                if (sourceFileTree != null && sourceFileTree.isEmpty() == false) {
                    PatternSet includePattern = getPatternSetFactory().create();
                    includePattern.include(INCLUDE_PATTERN);
                    for (File f : sourceFileTree.getAsFileTree().matching(includePattern)) {
                        boolean include = true;
                        for (PathMatcher matcher : excludesMatchers) {
                            if (matcher.matches(Paths.get(f.getCanonicalPath()))) {
                                include = false;
                            }
                        }
                        if (include) {
                            fileWriter.append(f.getCanonicalPath()).append(System.lineSeparator());
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Unexpected exception while generating the REST test source report", e);
            }
            logger.info("Writing REST test source report to [{}]", getOutputLocation().getAsFile().get().getAbsolutePath());
        }
    }

    @Override
    public void projects(String... paths) {
        this.includeProjects = Set.of(paths);
    }

    @Override
    public void excludes(String... exclude) {
        for (String pattern : exclude) {
            this.excludesMatchers.add(FileSystems.getDefault().getPathMatcher("glob:" + pattern));
        }
    }

    @Inject
    protected Factory<PatternSet> getPatternSetFactory() {
        throw new UnsupportedOperationException();
    }

    private Optional<SourceSet> getTestSourceSet(Project project) {
        return Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(SourceSet.TEST_SOURCE_SET_NAME));
    }

    private Optional<SourceSet> getMainSourceSet(Project project) {
        return Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME));
    }
}
