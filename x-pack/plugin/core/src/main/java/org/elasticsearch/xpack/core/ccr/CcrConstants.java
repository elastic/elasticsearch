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
    public static final String CCR_CUSTOM_METADATA_KEY = "ccr";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY = "leader_index_uuid";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS = "leader_index_shard_history_uuids";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY = "leader_index_name";
    public static final String CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY = "remote_cluster_name";

    public static final LicensedFeature.Momentary CCR_FEATURE = LicensedFeature.momentary(null, "ccr", License.OperationMode.PLATINUM);

    // no construction
    private CcrConstants() {}
}
