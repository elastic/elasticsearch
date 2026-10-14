/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.settings.Setting;

public enum BulkRoutingPolicy {
    PER_DOCUMENT,
    PER_BULK,
    LOCAL;

    public static final Setting<BulkRoutingPolicy> SETTING = Setting.enumSetting(
        BulkRoutingPolicy.class,
        "indices.bulk_routing.policy",
        LOCAL,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
}
