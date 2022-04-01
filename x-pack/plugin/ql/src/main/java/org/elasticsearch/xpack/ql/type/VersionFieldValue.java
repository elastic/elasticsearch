/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.xpack.versionfield.VersionEncoder;

import java.util.Objects;

public final class VersionFieldValue implements Comparable<VersionFieldValue> {
    private final String stringValue;
    private final VersionEncoder.EncodedVersion encoded;

    public VersionFieldValue(String str) {
        if (str == null) {
            throw new IllegalArgumentException("Invalid version field value: null");
        }
        stringValue = str;
        encoded = VersionEncoder.encodeVersion(str);
    }

    @Override
    public int compareTo(VersionFieldValue o) {
        return this.encoded.bytesRef.compareTo(o.encoded.bytesRef);
    }

    @Override
    public String toString() {
        return stringValue;
    }

    public boolean isValid() {
        return encoded.isLegal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionFieldValue that = (VersionFieldValue) o;
        return Objects.equals(stringValue, that.stringValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stringValue);
    }

}
