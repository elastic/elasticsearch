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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

public class ScriptCacheTests extends ESTestCase {
    // even though circuit breaking is allowed to be configured per minute, we actually weigh this over five minutes
    // simply by multiplying by five, so even setting it to one, requires five compilations to break
    public void testCompilationCircuitBreaking() throws Exception {
        ScriptCache cache = new ScriptCache(
            ScriptService.SCRIPT_CACHE_SIZE_SETTING.get(Settings.EMPTY),
            ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.get(Settings.EMPTY),
            ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.get(Settings.EMPTY)
        );
        cache.setMaxCompilationRate(Tuple.tuple(1, TimeValue.timeValueMinutes(1)));
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> cache.checkCompilationLimit());
        cache.setMaxCompilationRate(Tuple.tuple(2, TimeValue.timeValueMinutes(1)));
        cache.checkCompilationLimit(); // should pass
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, () -> cache.checkCompilationLimit());
        int count = randomIntBetween(5, 50);
        cache.setMaxCompilationRate(Tuple.tuple(count, TimeValue.timeValueMinutes(1)));
        for (int i = 0; i < count; i++) {
            cache.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, () -> cache.checkCompilationLimit());
        cache.setMaxCompilationRate(Tuple.tuple(0, TimeValue.timeValueMinutes(1)));
        expectThrows(CircuitBreakingException.class, () -> cache.checkCompilationLimit());
        cache.setMaxCompilationRate(Tuple.tuple(Integer.MAX_VALUE, TimeValue.timeValueMinutes(1)));
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            cache.checkCompilationLimit();
        }
    }

}
