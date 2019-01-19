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

package org.elasticsearch;

/**
 * Provides a static final field that can be used to check if assertions are enabled. Since this field might be used elsewhere to check if
 * assertions are enabled, if you are running with assertions enabled for specific packages or classes, you should enable assertions on this
 * class too (e.g., {@code -ea org.elasticsearch.Assertions -ea org.elasticsearch.cluster.service.MasterService}).
 */
public final class Assertions {

    private Assertions() {

    }

    public static final boolean ENABLED;

    static {
        boolean enabled = false;
        /*
         * If assertions are enabled, the following line will be evaluated and enabled will have the value true, otherwise when assertions
         * are disabled enabled will have the value false.
         */
        // noinspection ConstantConditions,AssertWithSideEffects
        assert enabled = true;
        // noinspection ConstantConditions
        ENABLED = enabled;
    }

}
