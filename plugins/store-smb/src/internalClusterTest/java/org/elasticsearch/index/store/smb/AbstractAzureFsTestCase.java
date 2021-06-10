/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.smb;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugin.store.smb.SMBStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

public abstract class AbstractAzureFsTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SMBStorePlugin.class);
    }

    public void testAzureFs() {
        // Create an index and index some documents
        createIndex("test");
        long nbDocs = randomIntBetween(10, 1000);
        for (long i = 0; i < nbDocs; i++) {
            indexDoc("test", "" + i, "foo", "bar");
        }
        refresh();
        SearchResponse response = client().prepareSearch("test").get();
        assertThat(response.getHits().getTotalHits().value, is(nbDocs));
    }
}
