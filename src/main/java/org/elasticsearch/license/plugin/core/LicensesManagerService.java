/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.inject.ImplementedBy;
import org.elasticsearch.license.core.ESLicense;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.license.plugin.core.LicensesService.DeleteLicenseRequestHolder;
import static org.elasticsearch.license.plugin.core.LicensesService.LicensesUpdateResponse;
import static org.elasticsearch.license.plugin.core.LicensesService.PutLicenseRequestHolder;

@ImplementedBy(LicensesService.class)
public interface LicensesManagerService {

    //TODO: documentation

    public void registerLicenses(final PutLicenseRequestHolder requestHolder, final ActionListener<LicensesUpdateResponse> listener);

    public void unregisterLicenses(final DeleteLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener);

    public LicensesStatus checkLicenses(Set<ESLicense> licenses);

    public Set<String> enabledFeatures();

    public List<ESLicense> getLicenses();
}
