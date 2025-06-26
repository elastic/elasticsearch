/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.versions;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.TransportVersionsGroup;

import java.util.List;

public class TestTV extends TransportVersionsGroup {
    public static final TestTV INSTANCE = new TestTV();

    static {
        TransportVersions.register(INSTANCE);
    }

    private TestTV() {
        super(List.of(new TransportVersion(9_111_0_00), new TransportVersion(8_111_0_00)));
    }
}
