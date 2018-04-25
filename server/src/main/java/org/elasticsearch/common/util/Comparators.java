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

package org.elasticsearch.common.util;

import java.util.Comparator;

/**
 * {@link Comparator}-related utility methods.
 */
public enum Comparators {
    ;

    /**
     * Compare <code>d1</code> against <code>d2</code>, pushing {@value Double#NaN} at the bottom.
     */
    public static int compareDiscardNaN(double d1, double d2, boolean asc) {
        if (Double.isNaN(d1)) {
            return Double.isNaN(d2) ? 0 : 1;
        } else if (Double.isNaN(d2)) {
            return -1;
        } else {
            return asc ? Double.compare(d1, d2) : Double.compare(d2, d1);
        }
    }

}
