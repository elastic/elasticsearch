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

package org.elasticsearch.test;

import java.util.Locale;

/**
 * All the supported versions of the tests for backwards compatibility reasons.
 * Can be used to enable/disable older behaviours that differ from the current ones.
 */
public enum TestVersion {

    @Deprecated
    DEFAULT_NUM_SHARDS_REPLICAS,

    RANDOM_NUM_SHARDS_REPLICAS;

    public boolean onOrAfter(TestVersion other) {
        return compareTo(other) >= 0;
    }

    public static TestVersion parse(String testVersion, TestVersion def) {
        if (testVersion == null) {
            return def;
        }
        String parsedMatchVersion = testVersion.toUpperCase(Locale.ROOT);
        return TestVersion.valueOf(parsedMatchVersion);
    }
}
