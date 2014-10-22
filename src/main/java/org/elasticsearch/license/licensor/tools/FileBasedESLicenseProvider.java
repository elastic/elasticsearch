/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.manager.ESLicenseProvider;
import org.elasticsearch.license.manager.Utils;

import java.util.Map;
import java.util.Set;

/**
 */
public class FileBasedESLicenseProvider implements ESLicenseProvider {
    private ImmutableMap<String, ESLicense> esLicenses;

    public FileBasedESLicenseProvider(Set<ESLicense> esLicenses) {
        this.esLicenses = Utils.reduceAndMap(esLicenses);
    }

    @Override
    public ESLicense getESLicense(String feature) {
        return esLicenses.get(feature);
    }

    @Override
    public Map<String, ESLicense> getEffectiveLicenses() {
        return esLicenses;
    }

    // For testing
    public void setLicenses(Set<ESLicense> esLicenses) {
        this.esLicenses = Utils.reduceAndMap(esLicenses);
    }
}
