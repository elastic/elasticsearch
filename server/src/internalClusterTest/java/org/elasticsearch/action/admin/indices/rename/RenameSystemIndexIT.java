/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.indices.TestSystemIndexDescriptor;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/**
 * System indices are managed by the features that own them and are addressed by fixed names, so a rename must not move an index
 * into or out of a system index pattern.
 */
public class RenameSystemIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestSystemIndexPlugin.class);
    }

    public void testRenameOfSystemIndexIsRefused() {
        assertAcked(indicesAdmin().prepareCreate(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME));
        ensureGreen(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME);
        final var e = expectThrows(
            IllegalArgumentException.class,
            () -> rename(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME, "no-longer-a-system-index")
        );
        assertThat(e.getMessage(), containsString("cannot rename system index"));
        assertTrue(indexExists(TestSystemIndexDescriptor.PRIMARY_INDEX_NAME));
    }

    public void testRenameIntoSystemIndexPatternIsRefused() {
        createIndex("regular", 1, 0, false);
        final var e = expectThrows(IllegalArgumentException.class, () -> rename("regular", TestSystemIndexDescriptor.INDEX_NAME + "-2"));
        assertThat(e.getMessage(), containsString("system index"));
        assertTrue(indexExists("regular"));
    }

    private void rename(String source, String target) {
        client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, source, target)
        ).actionGet();
    }
}
