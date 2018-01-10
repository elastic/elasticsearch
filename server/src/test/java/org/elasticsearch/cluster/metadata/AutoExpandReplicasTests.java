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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class AutoExpandReplicasTests extends ESTestCase {

    public void testParseSettings() {
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "0-5").build());
        assertEquals(0, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(8));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "0-all").build());
        assertEquals(0, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(6));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-all").build());
        assertEquals(1, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(6));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

    }

    public void testInvalidValues() {
        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom] at index -1", ex.getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [1-boom] at index 1", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom-1").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom-1] at index 4", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "2-1").build());
        } catch (IllegalArgumentException ex) {
            assertEquals("[index.auto_expand_replicas] minReplicas must be =< maxReplicas but wasn't 2 > 1", ex.getMessage());
        }

    }
}
