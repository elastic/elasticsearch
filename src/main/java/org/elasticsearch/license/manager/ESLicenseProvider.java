/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;


import org.elasticsearch.license.core.ESLicense;

import java.util.Map;


public interface ESLicenseProvider {

    ESLicense getESLicense(String feature);

    Map<String, ESLicense> getEffectiveLicenses();
}
