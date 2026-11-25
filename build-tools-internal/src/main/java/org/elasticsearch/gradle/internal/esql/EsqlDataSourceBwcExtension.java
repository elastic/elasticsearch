/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql;

import org.elasticsearch.gradle.Version;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for owner-module ES|QL data-source BWC tasks.
 */
public abstract class EsqlDataSourceBwcExtension {

    /** Earliest old distribution against which the owner suite can run. */
    public abstract Property<Version> getMinimumVersion();

    /** Source set containing the owning parameterized suite. */
    public abstract Property<String> getSourceSetName();

    /** Optional test-class filters for a mixed-purpose source set. */
    public abstract ListProperty<String> getClassFilters();

    /** Coordinator directions supported by the suite. */
    public abstract ListProperty<String> getCoordinatorModes();

    /** Additional system properties copied to every versioned task. */
    public abstract MapProperty<String, String> getSystemProperties();

    private final List<Exclusion> exclusions = new ArrayList<>();

    /** Sets {@link #getMinimumVersion()} from its string form. */
    public void minimumVersion(String version) {
        getMinimumVersion().set(Version.fromString(version));
    }

    /** Sets the owning test source set. */
    public void sourceSet(String sourceSetName) {
        getSourceSetName().set(sourceSetName);
    }

    /** Adds one or more test-class filters. */
    public void classFilter(String... patterns) {
        getClassFilters().addAll(patterns);
    }

    /** Replaces the supported coordinator modes. */
    public void coordinators(String... modes) {
        getCoordinatorModes().set(List.of(modes));
    }

    /** Adds a system property to every generated task. */
    public void systemProperty(String name, String value) {
        getSystemProperties().put(name, value);
    }

    /**
     * Excludes a class pattern with an explicit owner and reason.
     */
    public void exclude(String pattern, String owner, String reason) {
        if (pattern == null || pattern.isBlank()) {
            throw new IllegalArgumentException("A BWC exclusion must have a class pattern");
        }
        if (owner == null || owner.isBlank()) {
            throw new IllegalArgumentException("BWC exclusion [" + pattern + "] must have an owner");
        }
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("BWC exclusion [" + pattern + "] must have a reason");
        }
        exclusions.add(new Exclusion(pattern, owner, reason));
    }

    List<Exclusion> getExclusions() {
        return Collections.unmodifiableList(exclusions);
    }

    record Exclusion(String pattern, String owner, String reason) {}
}
