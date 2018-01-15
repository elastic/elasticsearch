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
package org.elasticsearch.cloud.gce;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cloud.gce.GceInstancesService.CONNECTION_TIMEOUT_SETTING;
import static org.elasticsearch.cloud.gce.GceInstancesService.READ_TIMEOUT_SETTING;
import static org.elasticsearch.cloud.gce.GceInstancesServiceImpl.timeoutToMillis;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class GceInstancesServiceImplTests extends ESTestCase {

    private final Setting<TimeValue> setting = randomBoolean() ? CONNECTION_TIMEOUT_SETTING : READ_TIMEOUT_SETTING;

    public void testTimeoutToMillisWithDefault() {
        assertThat(timeoutToMillis(Settings.EMPTY, setting), notNullValue());
        assertThat(timeoutToMillis(Settings.builder().putNull(setting.getKey()).build(), setting), notNullValue());
    }

    public void testTimeoutToMillisWithInifiniteTimeout() {
        assertThat(timeoutToMillis(Settings.builder().put(setting.getKey(), "-1").build(), setting), equalTo(0));
    }

    public void testTimeoutToMillis() {
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 60));
        assertThat(timeoutToMillis(Settings.builder().put(setting.getKey(), timeout).build(), setting), equalTo((int) timeout.getMillis()));
    }

    public void testTimeoutToMillisWithWrongTimeout() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> timeoutToMillis(Settings.builder().put(setting.getKey(), "-123s").build(), setting));
        assertThat(exception.getMessage(),
            equalTo("Timeout [" + setting.getKey() + "] must be greater than zero (or equal to -1 for infinite timeout)"));
    }
}
