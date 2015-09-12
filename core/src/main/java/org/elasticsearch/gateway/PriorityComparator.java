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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;

import java.util.Comparator;

/**
 * A comparator that compares ShardRouting based on it's indexes priority (index.priority),
 * it's creation date (index.creation_date), or eventually by it's index name in reverse order.
 * We try to recover first shards from an index with the highest priority, if that's the same
 * we try to compare the timestamp the index is created and pick the newer first (time-based indices,
 * here the newer indices matter more). If even that is the same, we compare the index name which is useful
 * if the date is baked into the index name. ie logstash-2015.05.03.
 */
public abstract class PriorityComparator implements Comparator<ShardRouting> {

    @Override
    public final int compare(ShardRouting o1, ShardRouting o2) {
        final String o1Index = o1.index();
        final String o2Index = o2.index();
        int cmp = 0;
        if (o1Index.equals(o2Index) == false) {
            final Settings settingsO1 = getIndexSettings(o1Index);
            final Settings settingsO2 = getIndexSettings(o2Index);
            cmp = Long.compare(priority(settingsO2), priority(settingsO1));
            if (cmp == 0) {
                cmp = Long.compare(timeCreated(settingsO2), timeCreated(settingsO1));
                if (cmp == 0) {
                    cmp = o2Index.compareTo(o1Index);
                }
            }
        }
        return cmp;
    }

    private int priority(Settings settings) {
        return settings.getAsInt(IndexMetaData.SETTING_PRIORITY, 1);
    }

    private long timeCreated(Settings settings) {
        return settings.getAsLong(IndexMetaData.SETTING_CREATION_DATE, -1l);
    }

    protected abstract Settings getIndexSettings(String index);

    /**
     * Returns a PriorityComparator that uses the RoutingAllocation index metadata to access the index setting per index.
     */
    public static PriorityComparator getAllocationComparator(final RoutingAllocation allocation) {
        return new PriorityComparator() {
            @Override
            protected Settings getIndexSettings(String index) {
                IndexMetaData indexMetaData = allocation.metaData().index(index);
                return indexMetaData.getSettings();
            }
        };
    }
}
