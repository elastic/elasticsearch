/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import org.elasticsearch.license.core.ESLicenses;

import static org.elasticsearch.license.core.ESLicenses.ESLicense;
import static org.elasticsearch.license.core.ESLicenses.FeatureType;

public interface ESLicenseProvider {

    ESLicense getESLicense(FeatureType featureType);

    ESLicenses getEffectiveLicenses();
}
