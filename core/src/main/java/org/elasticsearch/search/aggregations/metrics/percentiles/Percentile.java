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

package org.elasticsearch.search.aggregations.metrics.percentiles;

import java.util.Objects;

public class Percentile {

    private final double percent;
    private final double value;

    public Percentile(double percent, double value) {
        this.percent = percent;
        this.value = value;
    }

    public double getPercent() {
        return percent;
    }

    public double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Percentile that = (Percentile) o;
        return Double.compare(that.percent, percent) == 0
                && Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(percent, value);
    }
}