/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.precommit;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Represent rules for tests enforced by the @{link {@link TestingConventionsTasks}}
 *
 * Rules are identified by name, tests must have this name as a suffix and implement one of the base classes
 * and be part of all the specified tasks.
 */
public class TestingConventionRule implements Serializable {

    private final String suffix;

    private Set<String> baseClasses = new HashSet<>();

    private Set<Pattern> taskNames = new HashSet<>();

    public TestingConventionRule(String suffix) {
        this.suffix = suffix;
    }

    public String getSuffix() {
        return suffix;
    }

    /**
     * Alias for @{link getSuffix} as Gradle requires a name property
     *
     */
    public String getName() {
        return suffix;
    }

    public void baseClass(String clazz) {
        baseClasses.add(clazz);
    }

    public void setBaseClasses(Collection<String> baseClasses) {
        this.baseClasses.clear();
        this.baseClasses.addAll(baseClasses);
    }

    public void taskName(Pattern expression) {
        taskNames.add(expression);
    }

    public void taskName(String expression) {
        taskNames.add(Pattern.compile(expression));
    }

    public void setTaskNames(Collection<Pattern> expressions) {
        taskNames.clear();
        taskNames.addAll(expressions);
    }

    public Set<String> getBaseClasses() {
        return baseClasses;
    }

    public Set<Pattern> getTaskNames() {
        return taskNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestingConventionRule that = (TestingConventionRule) o;
        return Objects.equals(suffix, that.suffix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(suffix);
    }
}
