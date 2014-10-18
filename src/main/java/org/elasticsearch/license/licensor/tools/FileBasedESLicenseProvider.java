/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.elasticsearch.license.manager.ESLicenseProvider;

import java.util.Set;

/**
 */
public class FileBasedESLicenseProvider implements ESLicenseProvider {
    private ESLicenses esLicenses;

    public FileBasedESLicenseProvider(ESLicenses esLicenses) {
        this.esLicenses = esLicenses;
    }

    public FileBasedESLicenseProvider(Set<ESLicenses> esLicensesSet) {
        this(merge(esLicensesSet));
    }

    @Override
    public ESLicenses.ESLicense getESLicense(ESLicenses.FeatureType featureType) {
        return esLicenses.get(featureType);
    }

    @Override
    public ESLicenses getEffectiveLicenses() {
        return esLicenses;
    }

    // For testing
    public void setLicenses(ESLicenses esLicenses) {
        this.esLicenses = esLicenses;
    }

    private static ESLicenses merge(Set<ESLicenses> esLicensesSet) {
        ESLicenses mergedLicenses = null;
        for (ESLicenses licenses : esLicensesSet) {
            mergedLicenses = LicenseBuilders.merge(mergedLicenses, licenses);
        }
        return mergedLicenses;
    }

}
