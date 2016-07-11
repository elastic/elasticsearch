/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.license.core.License;

import java.util.List;

public interface LicensesManagerService {

    /**
     * @return current {@link LicenseState}
     */
    LicenseState licenseState();

    /**
     * @return the currently active license, or {@code null} if no license is currently installed
     */
    License getLicense();
}
