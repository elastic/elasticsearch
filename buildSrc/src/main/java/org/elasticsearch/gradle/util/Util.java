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
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.util.PatternFilterable;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /**
     * @param project The project to look for resources.
     * @param filter  Optional filter function to filter the returned resources
     * @return Returns the {@link FileTree} for main resources from Java projects. Returns null if no files exist.
     */
    @Nullable
    public static FileTree getJavaMainSourceResources(Project project, Action<? super PatternFilterable> filter) {
        final Optional<FileTree> mainFileTree = getJavaMainSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        return mainFileTree.map(files -> files.matching(filter)).orElse(null);
    }

    /**
     * @param project The project to look for resources.
     * @param filter  Optional filter function to filter the returned resources
     * @return Returns the {@link FileTree} for test resources from Java projects. Returns null if no files exist.
     */
    @Nullable
    public static FileTree getJavaTestSourceResources(Project project, Action<? super PatternFilterable> filter) {
        final Optional<FileTree> testFileTree = getJavaTestSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        return testFileTree.map(files -> files.matching(filter)).orElse(null);
    }

    /**
     * @param project The project to look for resources.
     * @param filter  Optional filter function to filter the returned resources
     * @return Returns the combined {@link FileTree} for test and main resources from Java projects. Returns null if no files exist.
     */
    @Nullable
    public static FileTree getJavaTestAndMainSourceResources(Project project, Action<? super PatternFilterable> filter) {
        final Optional<FileTree> testFileTree = getJavaTestSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        final Optional<FileTree> mainFileTree = getJavaMainSourceSet(project).map(SourceSet::getResources).map(FileTree::getAsFileTree);
        if (testFileTree.isPresent() && mainFileTree.isPresent()) {
            return testFileTree.get().plus(mainFileTree.get()).matching(filter);
        } else if (mainFileTree.isPresent()) {
            return mainFileTree.get().matching(filter);
        } else if (testFileTree.isPresent()) {
            return testFileTree.get().matching(filter);
        }
        return null;
    }

    /**
     * @param project The project to look for test Java resources.
     * @return An Optional that contains the Java test SourceSet if it exists.
     */
    public static Optional<SourceSet> getJavaTestSourceSet(Project project) {
        return project.getConvention().findPlugin(JavaPluginConvention.class) == null
            ? Optional.empty()
            : Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(SourceSet.TEST_SOURCE_SET_NAME));
    }

    /**
     * @param project The project to look for main Java resources.
     * @return An Optional that contains the Java main SourceSet if it exists.
     */
    public static Optional<SourceSet> getJavaMainSourceSet(Project project) {
        return project.getConvention().findPlugin(JavaPluginConvention.class) == null
            ? Optional.empty()
            : Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME));
    }

    static final Pattern GIT_PATTERN = Pattern.compile("git@([^:]+):([^\\.]+)\\.git");

    /** Find the reponame. */
    public static String urlFromOrigin(String origin) {
        if (origin == null) {
            return null; // best effort, the url doesnt really matter, it is just required by maven central
        }
        if (origin.startsWith("https")) {
            return origin;
        }
        Matcher matcher = GIT_PATTERN.matcher(origin);
        if (matcher.matches()) {
            return String.format("https://%s/%s", matcher.group(1), matcher.group(2));
        } else {
            return origin; // best effort, the url doesnt really matter, it is just required by maven central
        }
    }

    public static Object toStringable(Supplier<String> getter) {
        return new Object() {
            @Override
            public String toString() {
                return getter.get();
            }
        };
    }
}
