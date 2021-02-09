/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class SystemIndexResetApiIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SystemIndexTestPlugin.class);
        return plugins;
    }

    /** Check that the reset method cleans up a feature */
    public void testResetSystemIndices() throws Exception {
        String sysindex = ".test-system-idx-1";
        String associndex = ".associated-idx-1";

        // put a document in a system index
        indexDoc(sysindex, "1", "purpose", "system index doc");
        refresh(sysindex);

        // put a document in associated index
        indexDoc(associndex, "1", "purpose", "associated index doc");
        refresh(associndex);

        // put a document in a normal index
        indexDoc("my_index", "1", "purpose", "normal index doc");
        refresh("my_index");

        // call the reset API
        ResetFeatureStateResponse apiResponse = client().execute(ResetFeatureStateAction.INSTANCE, new ResetFeatureStateRequest()).get();
        assertThat(apiResponse.getItemList(), contains(
            new ResetFeatureStateResponse.ResetFeatureStateStatus("SystemIndexTestPlugin", "SUCCESS")));

        // verify that both indices are gone
        Exception e1 = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex()
            .addIndices(sysindex)
            .get());

        assertThat(e1.getMessage(), containsString("no such index"));

        Exception e2 = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex()
            .addIndices(associndex)
            .get());

        assertThat(e2.getMessage(), containsString("no such index"));

        GetIndexResponse response = client().admin().indices().prepareGetIndex()
            .addIndices("my_index")
            .get();

        assertThat(response.getIndices(), arrayContaining("my_index"));
    }

    /**
     * A test plugin with patterns for system indices and associated indices.
     */
    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_PATTERN = ".test-system-idx*";
        public static final String ASSOCIATED_INDEX_PATTERN = ".associated-idx*";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_PATTERN, "System indices for tests"));
        }

        @Override
        public Collection<String> getAssociatedIndexPatterns() {
            return Collections.singletonList(ASSOCIATED_INDEX_PATTERN);
        }

        @Override
        public String getFeatureName() {
            return SystemIndicesSnapshotIT.SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin";
        }
    }
}
