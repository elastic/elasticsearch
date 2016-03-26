/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

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
    DISABLED
}
