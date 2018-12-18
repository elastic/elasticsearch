/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.index;

import org.elasticsearch.xpack.core.upgrade.IndexUpgradeCheckVersion;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class SystemIndicesNames {

    public static final String AUDIT_INDEX_NAME_PREFIX = ".security_audit_log";
    public static final String INTERNAL_SECURITY_INDEX = ".security-" + IndexUpgradeCheckVersion.UPRADE_VERSION;
    public static final String SECURITY_INDEX_NAME = ".security";
    private static final Set<String> index_names;

    static {
        Set<String> indexNames = new HashSet<>();
        indexNames.add(SECURITY_INDEX_NAME);
        indexNames.add(INTERNAL_SECURITY_INDEX);
        index_names = Collections.unmodifiableSet(indexNames);
    }

    public static Set<String> indexNames() {
        return index_names;
    }

    private SystemIndicesNames() {
    }

}
