/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;

public class SearchableSnapshotsConstants {

    // This should really be in the searchable-snapshots module, but ILM needs access to it
    // to short-circuit if not allowed. We should consider making the coupling looser,
    // perhaps through SPI.
    public static final LicensedFeature.Momentary SEARCHABLE_SNAPSHOT_FEATURE = LicensedFeature.momentary(
        null,
        "searchable-snapshots",
        License.OperationMode.ENTERPRISE
    );

}
