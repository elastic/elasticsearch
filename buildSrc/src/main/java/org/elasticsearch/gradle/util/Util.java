/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.util;

import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.util.PatternSet;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

public class Util {

    public static boolean getBooleanProperty(String property, boolean defaultValue) {
        String propertyValue = System.getProperty(property);
        if (propertyValue == null) {
            return defaultValue;
        }
        if ("true".equals(propertyValue)) {
            return true;
        } else if ("false".equals(propertyValue)) {
            return false;
        } else {
            throw new GradleException("Sysprop [" + property + "] must be [true] or [false] but was [" + propertyValue + "]");
        }
    }

    public static String getResourceContents(String resourcePath) {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(GlobalBuildInfoPlugin.class.getResourceAsStream(resourcePath)))
        ) {
            StringBuilder b = new StringBuilder();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (b.length() != 0) {
                    b.append('\n');
                }
                b.append(line);
            }

            return b.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Error trying to read classpath resource: " + resourcePath, e);
        }
    }

    public static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }

    public static URI getBuildSrcCodeSource() {
        try {
            return Util.class.getProtectionDomain().getCodeSource().getLocation().toURI();
        } catch (URISyntaxException e) {
            throw new GradleException("Error determining build tools JAR location", e);
        }
    }

    public static Function<FileTree, FileTree> filterByPatterns(
        PatternSet patternSet,
        @Nullable List<String> includePatterns,
        @Nullable List<String> excludePatterns
    ) {
        return inFiles -> {
            if (inFiles != null && inFiles.isEmpty() == false) {
                if (includePatterns != null) {
                    patternSet.include(includePatterns);
                }
                if (excludePatterns != null) {
                    patternSet.exclude(excludePatterns);
                }
                return inFiles.getAsFileTree().matching(patternSet);
            } else {
                return null;
            }
        };
    }

    /**
     * @param project The project to look for resources.
     * @param filter  Optional filter function to filter the returned resources
     * @return Returns the {@FileTree} for main resources from Java projects. Returns null if no files exist.
     */
    @Nullable
    public static FileTree getJavaMainSourceResources(Project project, @Nullable Function<FileTree, FileTree> filter) {
        if (filter == null) {
            filter = Function.identity();
        }
        final Optional<FileTree> mainFileTree = getJavaMainSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        return filter.apply(mainFileTree.orElse(null));
    }

    /**
     * @param project The project to look for resources.
     * @param filter  Optional filter function to filter the returned resources
     * @return Returns the {@FileTree} for test resources from Java projects. Returns null if no files exist.
     */
    @Nullable
    public static FileTree getJavaTestSourceResources(Project project, @Nullable Function<FileTree, FileTree> filter) {
        if (filter == null) {
            filter = Function.identity();
        }
        final Optional<FileTree> testFileTree = getJavaTestSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        return filter.apply(testFileTree.orElse(null));
    }

    /**
     * @param project The project to look for resources.
     * @param filter  Optional filter function to filter the returned resources
     * @return Returns the combined {@FileTree} for test and main resources from Java projects. Returns null if no files exist.
     */
    @Nullable
    public static FileTree getJavaTestAndMainSourceResources(Project project, @Nullable Function<FileTree, FileTree> filter) {
        if (filter == null) {
            filter = Function.identity();
        }
        final Optional<FileTree> testFileTree = getJavaTestSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        final Optional<FileTree> mainFileTree = getJavaMainSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        if (testFileTree.isPresent() && mainFileTree.isPresent()) {
            return filter.apply(testFileTree.get().plus(mainFileTree.get()));
        } else {
            return filter.apply(testFileTree.orElse(mainFileTree.orElse(null)));
        }
    }

    /**
     * @param project The project to look for test Java resources.
     * @return An Optional that contains the Java test SourceSet if it exists.
     */
    public static Optional<SourceSet> getJavaTestSourceSet(Project project) {
        return project.getConvention().findPlugin(JavaPluginConvention.class) == null
            ? Optional.ofNullable(null)
            : Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(SourceSet.TEST_SOURCE_SET_NAME));
    }

    /**
     * @param project The project to look for main Java resources.
     * @return An Optional that contains the Java main SourceSet if it exists.
     */
    public static Optional<SourceSet> getJavaMainSourceSet(Project project) {
        return project.getConvention().findPlugin(JavaPluginConvention.class) == null
            ? Optional.ofNullable(null)
            : Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME));
    }
}
