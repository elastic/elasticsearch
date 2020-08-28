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
package org.elasticsearch.client.ml.job.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public enum DetectorFunction {

    COUNT,
    LOW_COUNT,
    HIGH_COUNT,
    NON_ZERO_COUNT("nzc"),
    LOW_NON_ZERO_COUNT("low_nzc"),
    HIGH_NON_ZERO_COUNT("high_nzc"),
    DISTINCT_COUNT("dc"),
    LOW_DISTINCT_COUNT("low_dc"),
    HIGH_DISTINCT_COUNT("high_dc"),
    RARE,
    FREQ_RARE,
    INFO_CONTENT,
    LOW_INFO_CONTENT,
    HIGH_INFO_CONTENT,
    METRIC,
    MEAN,
    LOW_MEAN,
    HIGH_MEAN,
    AVG,
    LOW_AVG,
    HIGH_AVG,
    MEDIAN,
    LOW_MEDIAN,
    HIGH_MEDIAN,
    MIN,
    MAX,
    SUM,
    LOW_SUM,
    HIGH_SUM,
    NON_NULL_SUM,
    LOW_NON_NULL_SUM,
    HIGH_NON_NULL_SUM,
    VARP,
    LOW_VARP,
    HIGH_VARP,
    TIME_OF_DAY,
    TIME_OF_WEEK,
    LAT_LONG;

    private Set<String> shortcuts;

    DetectorFunction() {
        shortcuts = Collections.emptySet();
    }

    DetectorFunction(String... shortcuts) {
        this.shortcuts = Arrays.stream(shortcuts).collect(Collectors.toSet());
    }

    public String getFullName() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return getFullName();
    }

    public static DetectorFunction fromString(String op) {
        for (DetectorFunction function : values()) {
            if (function.getFullName().equals(op) || function.shortcuts.contains(op)) {
                return function;
            }
        }
        throw new IllegalArgumentException("Unknown detector function [" + op + "]");
    }
}
