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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Comparator;

/**
 * A comparator that compares {@link ShardRouting} instances based on various properties. Instances
 * are ordered as follows.
 * <ol>
 *     <li>First, system indices are ordered before non-system indices</li>
 *     <li>Then indices are ordered by their priority, in descending order (index.priority)</li>
 *     <li>Then newer indices are ordered before older indices, based on their creation date. This benefits
 *         time-series indices, where newer indices are considered more urgent (index.creation_date)</li>
 *     <li>Lastly the index names are compared, which is useful when a date is baked into the index
 *         name, e.g. <code>logstash-2015.05.03</code></li>
 * </ol>
 */
public abstract class PriorityComparator implements Comparator<ShardRouting> {

    @Override
    public final int compare(ShardRouting o1, ShardRouting o2) {
        final String o1Index = o1.getIndexName();
        final String o2Index = o2.getIndexName();
        int cmp = 0;
        if (o1Index.equals(o2Index) == false) {
            final IndexMetadata metadata01 = getMetadata(o1.index());
            final IndexMetadata metadata02 = getMetadata(o2.index());
            cmp = Boolean.compare(metadata02.isSystem(), metadata01.isSystem());

            if (cmp == 0) {
                final Settings settingsO1 = metadata01.getSettings();
                final Settings settingsO2 = metadata02.getSettings();
                cmp = Long.compare(priority(settingsO2), priority(settingsO1));

                if (cmp == 0) {
                    cmp = Long.compare(timeCreated(settingsO2), timeCreated(settingsO1));
                    if (cmp == 0) {
                        cmp = o2Index.compareTo(o1Index);
                    }
                }
            }
        }
        return cmp;
    }

    private static int priority(Settings settings) {
        return IndexMetadata.INDEX_PRIORITY_SETTING.get(settings);
    }

    private static long timeCreated(Settings settings) {
        return settings.getAsLong(IndexMetadata.SETTING_CREATION_DATE, -1L);
    }

    protected abstract IndexMetadata getMetadata(Index index);

    /**
     * Returns a PriorityComparator that uses the RoutingAllocation index metadata to access the index setting per index.
     */
    public static PriorityComparator getAllocationComparator(final RoutingAllocation allocation) {
        return new PriorityComparator() {
            @Override
            protected IndexMetadata getMetadata(Index index) {
                return allocation.metadata().getIndexSafe(index);
            }
        };
    }
}
