/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartBasicResponse;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.license.PutLicenseRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

/**
 * Interface to update the current license.
 * <b>This interface is not intended to be implemented by alternative implementations and exists for internal use only.</b>
 */
public interface MutableLicenseService extends LicenseService, LifecycleComponent {

    /**
     * Creates or updates the current license as defined by the request.
     */
    void registerLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener);

    /**
     * Removes the current license. Implementations should remove the current license and ensure that attempts to read returns
     * {@link LicensesMetadata#LICENSE_TOMBSTONE} if a license was removed. Additionally the {@link XPackLicenseState} must be updated.
     */
    void removeLicense(ActionListener<? extends AcknowledgedResponse> listener);

    /**
     * Installs a basic license.
     */
    void startBasicLicense(PostStartBasicRequest request, ActionListener<PostStartBasicResponse> listener);

    /**
     * Installs a trial license.
     */
    void startTrialLicense(PostStartTrialRequest request, ActionListener<PostStartTrialResponse> listener);

}
