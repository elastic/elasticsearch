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
package org.elasticsearch.recovery;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

public class RecoverySettingsTest extends ElasticsearchSingleNodeTest {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Test
    public void testAllSettingsAreDynamicallyUpdatable() {
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.fileChunkSize().bytesAsInt());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_TRANSLOG_OPS, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.translogOps());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_TRANSLOG_SIZE, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.translogSize().bytesAsInt());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.concurrentStreamPool().getMaximumPoolSize());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.concurrentSmallFileStreamPool().getMaximumPoolSize());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC, 0, new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(null, recoverySettings.rateLimiter());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.retryDelayStateSync().millis());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.retryDelayNetwork().millis());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.activityTimeout().millis());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.internalActionTimeout().millis());
            }
        });
        innerTestSettings(RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT, randomIntBetween(1, 200), new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, int expectedValue) {
                assertEquals(expectedValue, recoverySettings.internalActionLongTimeout().millis());
            }
        });

        innerTestSettings(RecoverySettings.INDICES_RECOVERY_COMPRESS, false, new Validator() {
            @Override
            public void validate(RecoverySettings recoverySettings, boolean expectedValue) {
                assertEquals(expectedValue, recoverySettings.compress());
            }
        });
    }

    private static class Validator {
        public void validate(RecoverySettings recoverySettings, int expectedValue) {
        }

        public void validate(RecoverySettings recoverySettings, boolean expectedValue) {
        }
    }

    private void innerTestSettings(String key, int newValue, Validator validator) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.builder().put(key, newValue)).get();
        validator.validate(getInstanceFromNode(RecoverySettings.class), newValue);
    }

    private void innerTestSettings(String key, boolean newValue, Validator validator) {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.builder().put(key, newValue)).get();
        validator.validate(getInstanceFromNode(RecoverySettings.class), newValue);
    }

}
