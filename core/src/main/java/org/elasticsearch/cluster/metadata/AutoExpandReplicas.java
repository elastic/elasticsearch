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

final class AutoExpandReplicas {
    // the value we recognize in the "max" position to mean all the nodes
    private static final String ALL_NODES_VALUE = "all";
    public static final Setting<AutoExpandReplicas> SETTING = new Setting<>(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "", (value) -> {
        final int min;
        final int max;
        if (Booleans.parseBoolean(value, true) == false) {
            return new AutoExpandReplicas(0, 0, false);
        }
        final int dash = value.indexOf('-');
        if (-1 == dash) {
            throw new IllegalArgumentException("Can't parse auto expand clause from " + dash);
        }
        final String sMin = value.substring(0, dash);
        try {
            min = Integer.parseInt(sMin);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Can't parse auto expand clause from " + dash, e);
        }
        String sMax = value.substring(dash + 1);
        if (sMax.equals(ALL_NODES_VALUE)) {
            max = Integer.MAX_VALUE;
        } else {
            try {
                max = Integer.parseInt(sMax);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Can't parse auto expand clause from " + dash, e);
            }
        }
        return new AutoExpandReplicas(min, max, true);
    }, true, Setting.Scope.INDEX);
    private final int minReplicas;
    private final int maxReplicas;
    private final boolean enabled;

    public AutoExpandReplicas(int minReplicas, int maxReplicas, boolean enabled) {
        if (minReplicas > maxReplicas) {
            throw new IllegalArgumentException("min must be >= max");
        }
        this.minReplicas = minReplicas;
        this.maxReplicas = maxReplicas;
        this.enabled = enabled;
    }

    public int getMinReplicas() {
        return minReplicas;
    }

    public int getMaxReplicas(int numDataNodes) {
        return Math.min(maxReplicas, numDataNodes-1);
    }

    @Override
    public String toString() {
        return enabled == false ? Boolean.toString(enabled) : minReplicas + "-" + maxReplicas;
    }

    public boolean isEnabled() {
        return enabled;
    }
}


