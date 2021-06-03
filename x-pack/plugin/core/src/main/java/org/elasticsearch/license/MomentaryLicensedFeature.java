/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

/**
 * A licensed feature which checking the license state
 * counts as a use of the feature.
 */
public class MomentaryLicensedFeature extends LicensedFeature {

    public MomentaryLicensedFeature(String name, License.OperationMode minimumOperationMode, boolean needsActive) {
        super(name, minimumOperationMode, needsActive);
    }
}
