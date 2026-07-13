/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.common.component.LifecycleComponent;

/**
 * Interface to read the current license. Consumers should generally not need to read the license directly and should instead
 * prefer {@link XPackLicenseState} and/or {@link LicensedFeature} to make license decisions.
 * <b>This interface is not intended to be implemented by alternative implementations and exists for internal use only.</b>
 */
public interface LicenseService extends LifecycleComponent {

    /**
     * Get the current license. Reading the license directly should generally be avoided and
     * license decisions should generally prefer {@link XPackLicenseState} and/or {@link LicensedFeature}.
     * @return the current license, null or {@link LicensesMetadata#LICENSE_TOMBSTONE} if no license is available.
     */
    License getLicense();

}
