/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

public interface LicenseServiceInterface<

    RemoveResponse extends ActionResponse // remove license response
> extends LifecycleComponent {

    // TODO: split this into a read an write interface
    void registerLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener);

    void removeLicense(ActionListener<RemoveResponse> listener);

    /**
     * @return true if the license was found to be expired, false otherwise. If license is null - return false.
     */
    boolean maybeExpireLicense(License license);

    License getLicense();

}
