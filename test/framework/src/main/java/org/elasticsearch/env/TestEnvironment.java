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

package org.elasticsearch.env;

import org.elasticsearch.common.settings.Settings;

/**
 * Provides a convenience method for tests to construct an Environment when the config path does not matter.
 * This is in the test framework to force people who construct an Environment in production code to think
 * about what the config path needs to be set to.
 */
public class TestEnvironment {

    private TestEnvironment() {
    }

    public static Environment newEnvironment(Settings settings) {
        return new Environment(settings, null);
    }
}
