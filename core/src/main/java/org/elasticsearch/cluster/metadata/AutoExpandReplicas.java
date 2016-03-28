/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

/**
 * This class acts as a functional wrapper around the <tt>index.auto_expand_replicas</tt> setting.
 * This setting or rather it's value is expanded into a min and max value which requires special handling
 * based on the number of datanodes in the cluster. This class handles all the parsing and streamlines the access to these values.
 */
final class AutoExpandReplicas {
    // the value we recognize in the "max" position to mean all the nodes
    private static final String ALL_NODES_VALUE = "all";
    public static final Setting<AutoExpandReplicas> SETTING = new Setting<>(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "false", (value) -> {
        final int min;
        final int max;
        if (Booleans.parseBoolean(value, true) == false) {
            return new AutoExpandReplicas(0, 0, false);
        }
        final int dash = value.indexOf('-');
        if (-1 == dash) {
            throw new IllegalArgumentException("failed to parse [" + IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS + "] from value: [" + value + "] at index " + dash);
        }
        final String sMin = value.substring(0, dash);
        try {
            min = Integer.parseInt(sMin);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse [" + IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS + "] from value: [" + value + "] at index "  + dash, e);
        }
        String sMax = value.substring(dash + 1);
        if (sMax.equals(ALL_NODES_VALUE)) {
            max = Integer.MAX_VALUE;
        } else {
            try {
                max = Integer.parseInt(sMax);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("failed to parse [" + IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS + "] from value: [" + value + "] at index "  + dash, e);
            }
        }
        return new AutoExpandReplicas(min, max, true);
    }, Property.Dynamic, Property.IndexScope);

    private final int minReplicas;
    private final int maxReplicas;
    private final boolean enabled;

    private AutoExpandReplicas(int minReplicas, int maxReplicas, boolean enabled) {
        if (minReplicas > maxReplicas) {
            throw new IllegalArgumentException("[" + IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS + "] minReplicas must be =< maxReplicas but wasn't " + minReplicas + " > "  + maxReplicas);
        }
        this.minReplicas = minReplicas;
        this.maxReplicas = maxReplicas;
        this.enabled = enabled;
    }

    int getMinReplicas() {
        return minReplicas;
    }

    int getMaxReplicas(int numDataNodes) {
        return Math.min(maxReplicas, numDataNodes-1);
    }

    @Override
    public String toString() {
        return enabled ? minReplicas + "-" + maxReplicas : "false";
    }

    boolean isEnabled() {
        return enabled;
    }
}


