/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

public interface LicenseServerInterface {

    void registerLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener);

    // DeleteLicenseRequest is not currently used except to satify the Action framework, update this interface if it is ever acutally used.
    void removeLicense(ActionListener<? extends AcknowledgedResponse> listener);

    /**
     *
     * @param license
     * @return true if the license was found to be expired, false otherwise. If license is null - return false.
     */
    boolean maybeExpireLicense(License license);

    License getLicense();
}
