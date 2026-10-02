/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;

/**
 * Shared randomization predicate for {@link DataType}, so the several test fixtures that pick a random
 * serializable type ({@link EsFieldTestUtils}, {@code ReferenceAttributeTestUtils}) filter it the same
 * way and cannot drift apart. A type is usable when it is supported on the current node, is not
 * under construction, and — when a target version is given — serializes on that version (the same gate
 * {@code DataType#writeTo} enforces).
 */
public final class RandomDataTypeUtils {

    private RandomDataTypeUtils() {}

    /**
     * Whether a random generator may emit {@code type}. {@code supportedOn} is the transport version the
     * generated value will be serialized to (or {@code null} for no version constraint).
     */
    public static boolean isSerializable(DataType type, TransportVersion supportedOn, boolean currentBuildIsSnapshot) {
        if (type.supportedVersion().supportedLocally() == false) {
            return false;
        }
        if (DataType.UNDER_CONSTRUCTION.contains(type)) {
            return false;
        }
        return supportedOn == null || type.supportedVersion().supportedOn(supportedOn, currentBuildIsSnapshot);
    }

    /** Picks a random {@link DataType} serializable on {@code supportedOn} (or unconstrained when {@code null}). */
    public static DataType randomSerializableDataType(TransportVersion supportedOn) {
        boolean isSnapshot = Build.current().isSnapshot();
        return randomValueOtherThanMany(t -> isSerializable(t, supportedOn, isSnapshot) == false, () -> randomFrom(DataType.types()));
    }
}
