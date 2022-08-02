/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.util.Objects;

/**
 * Base class for rollover request conditions
 */
public abstract class Condition<T> implements NamedWriteable, ToXContentFragment {

    /**
     * Describes the type of condition - a min_* condition (MIN) or max_* condition (MAX).
     */
    public enum Type {
        MIN,
        MAX
    }

    protected T value;
    protected final String name;
    protected final Type type;

    protected Condition(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Checks if this condition is available in a specific version.
     * This makes sure BWC when introducing a new condition which is not recognized by older versions.
     */
    boolean includedInVersion(Version version) {
        return true;
    }

    public abstract Result evaluate(Stats stats);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Condition<?> condition = (Condition<?>) o;
        return Objects.equals(value, condition.value) && Objects.equals(name, condition.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, name);
    }

    @Override
    public final String toString() {
        return "[" + name + ": " + value + "]";
    }

    public T value() {
        return value;
    }

    public String name() {
        return name;
    }

    public Type type() {
        return type;
    }

    /**
     * Holder for index stats used to evaluate conditions
     */
    public record Stats(
        long numDocs,
        long indexCreated,
        ByteSizeValue indexSize,
        ByteSizeValue maxPrimaryShardSize,
        long maxPrimaryShardDocs
    ) {}

    /**
     * Holder for evaluated condition result
     */
    public record Result(Condition<?> condition, boolean matched) {}
}
