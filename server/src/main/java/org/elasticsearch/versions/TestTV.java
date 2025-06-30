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
import org.elasticsearch.TransportVersionsGroup;

import java.util.List;

public class TestTV extends TransportVersionsGroup {
    private TestTV() {
        super(List.of(
            new TransportVersion(9_111_0_00), // main
            new TransportVersion(9_001_0_00), // 9.0
            new TransportVersion(8_111_0_00), //8.19
            new TransportVersion(8_001_0_00) // 8.18
        ));
    }
}
