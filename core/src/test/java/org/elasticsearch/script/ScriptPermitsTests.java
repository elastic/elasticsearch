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

package org.elasticsearch.script;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyList;

public class ScriptPermitsTests extends ESTestCase {
    public void testCompilationCircuitBreaking() throws Exception {
        ScriptContextRegistry contextRegistry = new ScriptContextRegistry(emptyList());
        ScriptSettings scriptSettings = new ScriptSettings(new ScriptEngineRegistry(emptyList()),
                null, contextRegistry);
        ScriptPermits permits = new ScriptPermits(Settings.EMPTY, scriptSettings, contextRegistry);
        permits.setMaxCompilationsPerMinute(1);
        permits.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        permits.setMaxCompilationsPerMinute(2);
        permits.checkCompilationLimit(); // should pass
        permits.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        int count = randomIntBetween(5, 50);
        permits.setMaxCompilationsPerMinute(count);
        for (int i = 0; i < count; i++) {
            permits.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        permits.setMaxCompilationsPerMinute(0);
        expectThrows(CircuitBreakingException.class, () -> permits.checkCompilationLimit());
        permits.setMaxCompilationsPerMinute(Integer.MAX_VALUE);
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            permits.checkCompilationLimit();
        }
    }
}
