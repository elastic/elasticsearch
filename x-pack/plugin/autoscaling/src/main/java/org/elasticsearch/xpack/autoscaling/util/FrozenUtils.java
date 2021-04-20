/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.util;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;

public class FrozenUtils {
    public static boolean isFrozenIndex(Settings indexSettings) {
        String tierPreference = DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(indexSettings);
        String[] preferredTiers = DataTierAllocationDecider.parseTierList(tierPreference);
        if (preferredTiers.length >= 1 && preferredTiers[0].equals(DataTier.DATA_FROZEN)) {
            assert preferredTiers.length == 1 : "frozen tier preference must be frozen only";
            return true;
        } else {
            return false;
        }
    }
}
