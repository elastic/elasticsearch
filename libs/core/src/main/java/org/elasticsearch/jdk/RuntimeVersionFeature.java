/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.jdk;

import org.elasticsearch.core.UpdateForV9;

public class RuntimeVersionFeature {
    private RuntimeVersionFeature() {}

    @UpdateForV9(owner = UpdateForV9.Owner.CORE_INFRA) // Remove once we removed all references to SecurityManager in code
    public static boolean isSecurityManagerAvailable() {
        return Runtime.version().feature() < 24;
    }
}
