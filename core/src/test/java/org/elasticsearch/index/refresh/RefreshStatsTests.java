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

package org.elasticsearch.index.refresh;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

public class RefreshStatsTests extends AbstractStreamableTestCase<RefreshStats> {
    @Override
    protected RefreshStats createTestInstance() {
        return new RefreshStats(randomNonNegativeLong(), randomNonNegativeLong(), between(0, Integer.MAX_VALUE));
    }

    @Override
    protected RefreshStats createBlankInstance() {
        return new RefreshStats();
    }

    @Override
    protected MutateFunction<RefreshStats> getMutateFunction() {
        return instance -> {
            long total = instance.getTotal();
            long totalInMillis = instance.getTotalTimeInMillis();
            int listeners = instance.getListeners();
            switch (randomInt(2)) {
            case 0:
                total += between(1, 2000);
                break;
            case 1:
                totalInMillis += between(1, 2000);
                break;
            case 2:
            default:
                listeners += between(1, 2000);
                break;
            }
            return new RefreshStats(total, totalInMillis, listeners);
        };
    }
}
