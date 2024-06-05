/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

/**
 * Indicates a class that represents a version id of some kind
 */
public interface VersionId<T extends VersionId<T>> extends Comparable<T> {
    /**
     * The version id this object represents
     */
    int id();

    default boolean after(T version) {
        return version.id() < id();
    }

    default boolean onOrAfter(T version) {
        return version.id() <= id();
    }

    default boolean before(T version) {
        return version.id() > id();
    }

    default boolean onOrBefore(T version) {
        return version.id() >= id();
    }

    default boolean between(T lowerInclusive, T upperExclusive) {
        if (upperExclusive.onOrBefore(lowerInclusive)) throw new IllegalArgumentException();
        return onOrAfter(lowerInclusive) && before(upperExclusive);
    }

    @Override
    default int compareTo(T o) {
        return Integer.compare(id(), o.id());
    }
}
