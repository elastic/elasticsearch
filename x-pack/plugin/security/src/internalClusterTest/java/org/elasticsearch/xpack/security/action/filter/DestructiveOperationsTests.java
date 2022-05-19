/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.filter;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.After;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DestructiveOperationsTests extends SecurityIntegTestCase {

    @After
    public void afterTest() {
        Settings settings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), (String) null).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

    public void testDeleteIndexDestructiveOperationsRequireName() {
        createIndex("index1");
        Settings settings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareDelete("*").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
            String[] indices = client().admin().indices().prepareGetIndex().setIndices("index1").get().getIndices();
            assertEquals(1, indices.length);
            assertEquals("index1", indices[0]);
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareDelete("*", "-index1").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
            String[] indices = client().admin().indices().prepareGetIndex().setIndices("index1").get().getIndices();
            assertEquals(1, indices.length);
            assertEquals("index1", indices[0]);
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareDelete("_all").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
            String[] indices = client().admin().indices().prepareGetIndex().setIndices("index1").get().getIndices();
            assertEquals(1, indices.length);
            assertEquals("index1", indices[0]);
        }

        // the "*,-*" pattern is specially handled because it makes a destructive action non-destructive
        assertAcked(client().admin().indices().prepareDelete("*", "-*"));
        assertAcked(client().admin().indices().prepareDelete("index1"));
    }

    public void testDestructiveOperationsDefaultBehaviour() {
        if (randomBoolean()) {
            Settings settings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
        }
        createIndex("index1", "index2");

        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                assertAcked(client().admin().indices().prepareClose("*"));
                assertAcked(client().admin().indices().prepareOpen("*"));
                assertAcked(client().admin().indices().prepareDelete("*"));
            }
            case 1 -> {
                assertAcked(client().admin().indices().prepareClose("_all"));
                assertAcked(client().admin().indices().prepareOpen("_all"));
                assertAcked(client().admin().indices().prepareDelete("_all"));
            }
            case 2 -> {
                assertAcked(client().admin().indices().prepareClose("*", "-index1"));
                assertAcked(client().admin().indices().prepareOpen("*", "-index1"));
                assertAcked(client().admin().indices().prepareDelete("*", "-index1"));
            }
            default -> throw new UnsupportedOperationException();
        }
    }

    public void testOpenCloseIndexDestructiveOperationsRequireName() {
        Settings settings = Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareClose("*").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareClose("*", "-index1").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareClose("_all").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareOpen("*").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareOpen("*", "-index1").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }
        {
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().prepareOpen("_all").get()
            );
            assertEquals("Wildcard expressions or all indices are not allowed", illegalArgumentException.getMessage());
        }

        // the "*,-*" pattern is specially handled because it makes a destructive action non-destructive
        assertAcked(client().admin().indices().prepareClose("*", "-*"));
        assertAcked(client().admin().indices().prepareOpen("*", "-*"));

        createIndex("index1");
        assertAcked(client().admin().indices().prepareClose("index1"));
        assertAcked(client().admin().indices().prepareOpen("index1"));
    }
}
