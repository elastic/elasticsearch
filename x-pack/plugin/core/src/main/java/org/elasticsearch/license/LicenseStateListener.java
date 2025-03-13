/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.license.internal.XPackLicenseStatus;

/**
 * Marker interface for callbacks that are invoked when the license state changes.
 */
@FunctionalInterface
public interface LicenseStateListener {

    /**
     * Callback when the license state changes. See {@link XPackLicenseState#update(XPackLicenseStatus)}.
     */
    void licenseStateChanged();

}
