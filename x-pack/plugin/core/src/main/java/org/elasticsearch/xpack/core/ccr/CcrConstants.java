/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr;

import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;

public class CcrConstants {

    public static final LicensedFeature.Momentary CCR_FEATURE = LicensedFeature.momentary(null, "ccr", License.OperationMode.PLATINUM);

    // no construction
    private CcrConstants() {}
}
