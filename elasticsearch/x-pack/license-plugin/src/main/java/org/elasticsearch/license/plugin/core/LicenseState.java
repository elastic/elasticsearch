/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;

import static org.elasticsearch.license.plugin.core.LicensesService.days;

/**
 * States of a registered licensee
 * based on the current license
 */
public enum LicenseState {

    /**
     * Active license is valid.
     *
     * When license expires
     * changes to {@link #GRACE_PERIOD}
     */
    ENABLED,

    /**
     * Active license expired
     * but grace period has not.
     *
     * When grace period expires
     * changes to {@link #DISABLED}.
     * When valid license is installed
     * changes back to {@link #ENABLED}
     */
    GRACE_PERIOD,

    /**
     * Grace period for active license
     * expired.
     *
     * When a valid license is installed
     * changes to {@link #ENABLED}, otherwise
     * remains unchanged
     */
    DISABLED;

    /**
     * Duration of grace period after a license has expired
     */
    public static final TimeValue GRACE_PERIOD_DURATION = days(7);

    public static LicenseState resolve(final License license, long time) {
        if (license == null) {
            return DISABLED;
        }
        if (license.issueDate() > time) {
            return DISABLED;
        }
        if (license.expiryDate() > time) {
            return ENABLED;
        }
        if ((license.expiryDate() + GRACE_PERIOD_DURATION.getMillis()) > time) {
            return GRACE_PERIOD;
        }
        return DISABLED;
    }
}
