/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions.util;

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.util.PatternFilterable;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Supplier;

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
        return project.getExtensions().findByName("java") == null
            ? Optional.empty()
            : Optional.ofNullable(getJavaSourceSets(project).findByName(SourceSet.TEST_SOURCE_SET_NAME));
    }

    /**
     * @param project The project to look for main Java resources.
     * @return An Optional that contains the Java main SourceSet if it exists.
     */
    public static Optional<SourceSet> getJavaMainSourceSet(Project project) {
        return isJavaExtensionAvailable(project)
            ? Optional.empty()
            : Optional.ofNullable(getJavaSourceSets(project).findByName(SourceSet.MAIN_SOURCE_SET_NAME));
    }

    private static boolean isJavaExtensionAvailable(Project project) {
        return project.getExtensions().getByType(JavaPluginExtension.class) == null;
    }


    public static Object toStringable(Supplier<String> getter) {
        return new Object() {
            @Override
            public String toString() {
                return getter.get();
            }
        };
    }

    public static SourceSetContainer getJavaSourceSets(Project project) {
        return project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets();
    }

}
