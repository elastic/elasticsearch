/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices.SecurityMainIndexMappingVersion;

import java.util.HashMap;
import java.util.Map;

public class SecurityMainIndexMappingVersionTests extends ESTestCase {

    public void testVersionIdUniqueness() {
        Map<Integer, SecurityMainIndexMappingVersion> ids = new HashMap<>();
        for (var version : SecurityMainIndexMappingVersion.values()) {
            var existing = ids.put(version.id(), version);
            if (existing != null) {
                fail(
                    "duplicate ID ["
                        + version.id()
                        + "] definition found in SecurityMainIndexMappingVersion for ["
                        + version
                        + "] and ["
                        + existing
                        + "]"
                );
            }
        }
    }
}
