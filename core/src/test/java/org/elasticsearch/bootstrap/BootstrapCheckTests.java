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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;

public class BootstrapCheckTests extends ESTestCase {

    public void testNonProductionMode() {
        runTest(this::runNonProductionModeTest);
    }

    private void runNonProductionModeTest(boolean snapshot) {
        // nothing should happen since we are in non-production mode
        BootstrapCheck.check(Settings.EMPTY, snapshot);
    }

    public void testFileDescriptorLimits() {
        runTest(this::runFileDescriptorLimitsTest);
    }

    private void runFileDescriptorLimitsTest(boolean snapshot) {
        long limit = snapshot ? 1 << 10: 1 << 16;
        AtomicLong maxFileDescriptorCount = new AtomicLong(limit - 1);
        BootstrapCheck.FileDescriptorCheck check = new BootstrapCheck.FileDescriptorCheck(snapshot) {
            @Override
            long getMaxFileDescriptorCount() {
                return maxFileDescriptorCount.get();
            }
        };

        try {
            BootstrapCheck.check(true, Collections.singletonList(check));
            fail("should have failed due to max file descriptors too low");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString("max file descriptors"));
        }

        maxFileDescriptorCount.set(limit + 1);

        BootstrapCheck.check(true, Collections.singletonList(check));
    }

    public void testEnforceLimits() {
        Set<Setting> enforceSettings = BootstrapCheck.enforceSettings();
        Setting setting = randomFrom(Arrays.asList(enforceSettings.toArray(new Setting[enforceSettings.size()])));
        Settings settings = Settings.builder().put(setting.getKey(), randomAsciiOfLength(8)).build();
        assertTrue(BootstrapCheck.enforceLimits(settings));
    }

    private void runTest(Consumer<Boolean> test) {
        test.accept(true);
        test.accept(false);
    }

}
