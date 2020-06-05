/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.persistence;

import org.elasticsearch.Version;

public final class TransformInternalIndexConstants {

    /* Constants for internal indexes of the transform plugin
     * (defined in core to provide wider access)
     *
     * Increase the version number for every mapping change, see TransformInternalIndex for details
     *
     * Together with increasing the version number please keep the following in sync:
     *
     *    - XPackRestTestConstants
     *    - yaml tests under x-pack/qa/
     *
     * (pro-tip: grep for the constant)
     */

    // internal index

    // version is not a rollover pattern, however padded because sort is string based
    public static final Version INDEX_VERSION_LAST_CHANGED = Version.V_7_7_0;
    public static final String INDEX_VERSION = "005";
    public static final String INDEX_PATTERN = ".transform-internal-";
    public static final String LATEST_INDEX_VERSIONED_NAME = INDEX_PATTERN + INDEX_VERSION;
    public static final String LATEST_INDEX_NAME = LATEST_INDEX_VERSIONED_NAME;
    public static final String INDEX_NAME_PATTERN = INDEX_PATTERN + "*";
    public static final String INDEX_NAME_PATTERN_DEPRECATED = ".data-frame-internal-*";

    // audit index
    // gh #49730: upped version of audit index to 000002
    public static final String AUDIT_TEMPLATE_VERSION = "000002";
    public static final String AUDIT_INDEX_PREFIX = ".transform-notifications-";
    public static final String AUDIT_INDEX_PATTERN = AUDIT_INDEX_PREFIX + "*";
    public static final String AUDIT_INDEX_DEPRECATED = ".data-frame-notifications-1";
    public static final String AUDIT_INDEX_PATTERN_DEPRECATED = ".data-frame-notifications-*";

    public static final String AUDIT_INDEX_READ_ALIAS = ".transform-notifications-read";
    public static final String AUDIT_INDEX = AUDIT_INDEX_PREFIX + AUDIT_TEMPLATE_VERSION;

    private TransformInternalIndexConstants() {}

}
