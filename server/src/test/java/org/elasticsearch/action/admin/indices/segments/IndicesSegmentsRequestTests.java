/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.util.Collection;

import static org.hamcrest.Matchers.is;

public class IndicesSegmentsRequestTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    @Before
    public void setupIndex() {
        Settings settings = Settings.builder()
            // don't allow any merges so that the num docs is the expected segments
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .build();
        createIndex("test", settings);

        int numDocs = scaledRandomIntBetween(100, 1000);
        for (int j = 0; j < numDocs; ++j) {
            String id = Integer.toString(j);
            client().prepareIndex("test").setId(id).setSource("text", "sometext").get();
        }
        client().admin().indices().prepareFlush("test").get();
        client().admin().indices().prepareRefresh().get();
    }

    /**
     * with the default IndicesOptions inherited from BroadcastOperationRequest this will raise an exception
     */
    public void testRequestOnClosedIndex() {
        client().admin().indices().prepareClose("test").get();
        try {
            client().admin().indices().prepareSegments("test").get();
            fail("Expected IndexClosedException");
        } catch (IndexClosedException e) {
            assertThat(e.getMessage(), is("closed"));
        }
    }

    /**
     * setting the "ignoreUnavailable" option prevents IndexClosedException
     */
    public void testRequestOnClosedIndexIgnoreUnavailable() {
        client().admin().indices().prepareClose("test").get();
        IndicesOptions defaultOptions = new IndicesSegmentsRequest().indicesOptions();
        IndicesOptions testOptions = IndicesOptions.fromOptions(true, true, true, false, defaultOptions);
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments("test").setIndicesOptions(testOptions).get();
        assertEquals(0, rsp.getIndices().size());
    }

    /**
     * by default IndicesOptions setting IndicesSegmentsRequest should not throw exception when no index present
     */
    public void testAllowNoIndex() {
        client().admin().indices().prepareDelete("test").get();
        IndicesSegmentResponse rsp = client().admin().indices().prepareSegments().get();
        assertEquals(0, rsp.getIndices().size());
    }
}
