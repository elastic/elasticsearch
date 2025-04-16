/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class RolloverRestCancellationIT extends BlockedSearcherRestCancellationTestCase {

    public void testRolloverRestCancellation() throws Exception {
        assertAcked(
            prepareCreate("test-000001").addAlias(new Alias("test-alias").writeIndex(true))
                .setSettings(Settings.builder().put(BLOCK_SEARCHER_SETTING.getKey(), true))
        );
        ensureGreen("test-000001");
        runTest(new Request(HttpPost.METHOD_NAME, "test-alias/_rollover"), RolloverAction.NAME);
    }
}
