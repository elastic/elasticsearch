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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.sort.SortOrder;

class CompositeValuesSourceConfig {
    private final String name;
    private final ValuesSource vs;
    private final int reverseMul;
    private final boolean canEarlyTerminate;

    CompositeValuesSourceConfig(String name, ValuesSource vs, SortOrder order, boolean canEarlyTerminate) {
        this.name = name;
        this.vs = vs;
        this.canEarlyTerminate = canEarlyTerminate;
        this.reverseMul = order == SortOrder.ASC ? 1 : -1;
    }

    String name() {
        return name;
    }

    ValuesSource valuesSource() {
        return vs;
    }

    /**
     * The sort order for the values source (e.g. -1 for descending and 1 for ascending).
     */
    int reverseMul() {
        assert reverseMul == -1 || reverseMul == 1;
        return reverseMul;
    }

    boolean canEarlyTerminate() {
        return canEarlyTerminate;
    }
}
