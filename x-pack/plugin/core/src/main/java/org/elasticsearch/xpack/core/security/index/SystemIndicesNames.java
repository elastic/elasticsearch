/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.index;

import org.elasticsearch.xpack.core.upgrade.IndexUpgradeCheckVersion;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class SystemIndicesNames {

    public static final String AUDIT_INDEX_NAME_PREFIX = ".security_audit_log";
    public static final String INTERNAL_SECURITY_INDEX = ".security-" + IndexUpgradeCheckVersion.UPRADE_VERSION;
    public static final String SECURITY_INDEX_NAME = ".security";
    private static final List<String> index_names;

    static {
        index_names = Collections.unmodifiableList(Arrays.asList(SECURITY_INDEX_NAME, INTERNAL_SECURITY_INDEX));
    }

    private SystemIndicesNames() {
    }

    public static List<String> indexNames() {
        return index_names;
    }

}
