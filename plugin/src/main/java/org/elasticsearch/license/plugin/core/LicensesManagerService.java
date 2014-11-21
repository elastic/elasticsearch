/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.inject.ImplementedBy;
import org.elasticsearch.license.core.License;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.license.plugin.core.LicensesService.*;

@ImplementedBy(LicensesService.class)
public interface LicensesManagerService {

    /**
     * Registers new licenses in the cluster
     * <p/>
     * This method can be only called on the master node. It tries to create a new licenses on the master
     * and if provided license(s) is VALID it is added to cluster state metadata {@link org.elasticsearch.license.plugin.core.LicensesMetaData}
     */
    public void registerLicenses(final PutLicenseRequestHolder requestHolder, final ActionListener<LicensesService.LicensesUpdateResponse> listener);

    /**
     * Remove only signed license(s) for provided features from the cluster state metadata
     */
    public void removeLicenses(final DeleteLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener);

    /**
     * @return the set of features that are currently enabled
     */
    public Set<String> enabledFeatures();

    /**
     * @return a list of licenses, contains one license (with the latest expiryDate) per registered features sorted by latest issueDate
     */
    public List<License> getLicenses();
}
