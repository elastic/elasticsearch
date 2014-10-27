/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;

import java.util.Map;
import java.util.Set;

/**
 */
public class FileBasedESLicenseProvider {
    private ImmutableMap<String, ESLicense> esLicenses;

    public FileBasedESLicenseProvider(Set<ESLicense> esLicenses) {
        this.esLicenses = ESLicenses.reduceAndMap(esLicenses);
    }

    public ESLicense getESLicense(String feature) {
        return esLicenses.get(feature);
    }

    public Map<String, ESLicense> getEffectiveLicenses() {
        return esLicenses;
    }

    // For testing
    public void setLicenses(Set<ESLicense> esLicenses) {
        this.esLicenses = ESLicenses.reduceAndMap(esLicenses);
    }
}
