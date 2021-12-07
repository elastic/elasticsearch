/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

/**
 * Script value class.
 * TODO(stu): implement {@code Comparable<Version>} based on {@code VersionEncoder#prefixDigitGroupsWithLength(String, BytesRefBuilder)}
 */
public class Version {
    protected String version;

    public Version(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return version;
    }
}
