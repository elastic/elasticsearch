/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.util;

import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class FrozenUtils {
    public static boolean isFrozenIndex(Settings indexSettings) {
        String tierPreference = DataTier.TIER_PREFERENCE_SETTING.get(indexSettings);
        List<String> preferredTiers = DataTier.parseTierList(tierPreference);
        if (preferredTiers.isEmpty() == false && preferredTiers.get(0).equals(DataTier.DATA_FROZEN)) {
            assert preferredTiers.size() == 1 : "frozen tier preference must be frozen only";
            return true;
        } else {
            return false;
        }
    }
}
