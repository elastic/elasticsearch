/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.lookup;

import java.util.Objects;

public class PainlessCast {

    /** Create a standard cast with no boxing/unboxing. */
    public static PainlessCast originalTypetoTargetType(Class<?> originalType, Class<?> targetType, boolean explicitCast) {
        Objects.requireNonNull(originalType);
        Objects.requireNonNull(targetType);

        return new PainlessCast(originalType, targetType, explicitCast, null, null, null, null);
    }

    /** Create a cast where the original type will be unboxed, and then the cast will be performed. */
    public static PainlessCast unboxOriginalType(
        Class<?> originalType,
        Class<?> targetType,
        boolean explicitCast,
        Class<?> unboxOriginalType
    ) {

        Objects.requireNonNull(originalType);
        Objects.requireNonNull(targetType);
        Objects.requireNonNull(unboxOriginalType);

        return new PainlessCast(originalType, targetType, explicitCast, unboxOriginalType, null, null, null);
    }

    /** Create a cast where the target type will be unboxed, and then the cast will be performed. */
    public static PainlessCast unboxTargetType(Class<?> originalType, Class<?> targetType, boolean explicitCast, Class<?> unboxTargetType) {

        Objects.requireNonNull(originalType);
        Objects.requireNonNull(targetType);
        Objects.requireNonNull(unboxTargetType);

        return new PainlessCast(originalType, targetType, explicitCast, null, unboxTargetType, null, null);
    }

    /** Create a cast where the original type will be boxed, and then the cast will be performed. */
    public static PainlessCast boxOriginalType(Class<?> originalType, Class<?> targetType, boolean explicitCast, Class<?> boxOriginalType) {

        Objects.requireNonNull(originalType);
        Objects.requireNonNull(targetType);
        Objects.requireNonNull(boxOriginalType);

        return new PainlessCast(originalType, targetType, explicitCast, null, null, boxOriginalType, null);
    }

    /** Create a cast where the target type will be boxed, and then the cast will be performed. */
    public static PainlessCast boxTargetType(Class<?> originalType, Class<?> targetType, boolean explicitCast, Class<?> boxTargetType) {

        Objects.requireNonNull(originalType);
        Objects.requireNonNull(targetType);
        Objects.requireNonNull(boxTargetType);

        return new PainlessCast(originalType, targetType, explicitCast, null, null, null, boxTargetType);
    }

    /** Create a cast where the original type is unboxed, cast to a target type, and the target type is boxed. */
    public static PainlessCast unboxOriginalTypeToBoxTargetType(boolean explicitCast, Class<?> unboxOriginalType, Class<?> boxTargetType) {

        Objects.requireNonNull(unboxOriginalType);
        Objects.requireNonNull(boxTargetType);

        return new PainlessCast(null, null, explicitCast, unboxOriginalType, null, null, boxTargetType);
    }

    public final Class<?> originalType;
    public final Class<?> targetType;
    public final boolean explicitCast;
    public final Class<?> unboxOriginalType;
    public final Class<?> unboxTargetType;
    public final Class<?> boxOriginalType;
    public final Class<?> boxTargetType;

    private PainlessCast(
        Class<?> originalType,
        Class<?> targetType,
        boolean explicitCast,
        Class<?> unboxOriginalType,
        Class<?> unboxTargetType,
        Class<?> boxOriginalType,
        Class<?> boxTargetType
    ) {

        this.originalType = originalType;
        this.targetType = targetType;
        this.explicitCast = explicitCast;
        this.unboxOriginalType = unboxOriginalType;
        this.unboxTargetType = unboxTargetType;
        this.boxOriginalType = boxOriginalType;
        this.boxTargetType = boxTargetType;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessCast that = (PainlessCast) object;

        return explicitCast == that.explicitCast
            && Objects.equals(originalType, that.originalType)
            && Objects.equals(targetType, that.targetType)
            && Objects.equals(unboxOriginalType, that.unboxOriginalType)
            && Objects.equals(unboxTargetType, that.unboxTargetType)
            && Objects.equals(boxOriginalType, that.boxOriginalType)
            && Objects.equals(boxTargetType, that.boxTargetType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalType, targetType, explicitCast, unboxOriginalType, unboxTargetType, boxOriginalType, boxTargetType);
    }
}
