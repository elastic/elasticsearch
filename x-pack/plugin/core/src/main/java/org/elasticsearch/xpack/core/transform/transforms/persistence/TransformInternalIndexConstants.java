/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
     *    - upgrade tests under x-pack/qa/rolling-upgrade
     *    - TransformSurvivesUpgradeIT
     *
     * (pro-tip: grep for the constant)
     */

    // internal index
    public static final String TRANSFORM_PREFIX = ".transform-";
    public static final String TRANSFORM_PREFIX_DEPRECATED = ".data-frame-";

    // version is not a rollover pattern, however padded because sort is string based
    public static final Version INDEX_VERSION_LAST_CHANGED = Version.V_7_13_0;
    public static final String INDEX_VERSION = "007";
    public static final String INDEX_PATTERN = TRANSFORM_PREFIX + "internal-";
    public static final String LATEST_INDEX_VERSIONED_NAME = INDEX_PATTERN + INDEX_VERSION;
    public static final String LATEST_INDEX_NAME = LATEST_INDEX_VERSIONED_NAME;
    public static final String INDEX_NAME_PATTERN = INDEX_PATTERN + "*";
    public static final String INDEX_NAME_PATTERN_DEPRECATED = TRANSFORM_PREFIX_DEPRECATED + "internal-*";

    // audit index
    // gh #49730: upped version of audit index to 000002
    public static final String AUDIT_TEMPLATE_VERSION = "000002";
    public static final String AUDIT_INDEX_PREFIX = TRANSFORM_PREFIX + "notifications-";
    public static final String AUDIT_INDEX_PATTERN = AUDIT_INDEX_PREFIX + "*";
    public static final String AUDIT_INDEX_DEPRECATED = TRANSFORM_PREFIX_DEPRECATED + "notifications-1";
    public static final String AUDIT_INDEX_PATTERN_DEPRECATED = TRANSFORM_PREFIX_DEPRECATED + "notifications-*";

    public static final String AUDIT_INDEX_READ_ALIAS = TRANSFORM_PREFIX + "notifications-read";
    public static final String AUDIT_INDEX = AUDIT_INDEX_PREFIX + AUDIT_TEMPLATE_VERSION;

    private TransformInternalIndexConstants() {}

}
