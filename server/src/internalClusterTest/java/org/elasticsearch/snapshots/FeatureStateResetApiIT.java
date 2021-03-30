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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class FeatureStateResetApiIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SystemIndexTestPlugin.class);
        plugins.add(SecondSystemIndexTestPlugin.class);
        return plugins;
    }

    /** Check that the reset method cleans up a feature */
    public void testResetSystemIndices() throws Exception {
        String systemIndex1 = ".test-system-idx-1";
        String systemIndex2 = ".second-test-system-idx-1";
        String associatedIndex = ".associated-idx-1";

        // put a document in a system index
        indexDoc(systemIndex1, "1", "purpose", "system index doc");
        refresh(systemIndex1);

        // put a document in a second system index
        indexDoc(systemIndex2, "1", "purpose", "second system index doc");
        refresh(systemIndex2);

        // put a document in associated index
        indexDoc(associatedIndex, "1", "purpose", "associated index doc");
        refresh(associatedIndex);

        // put a document in a normal index
        indexDoc("my_index", "1", "purpose", "normal index doc");
        refresh("my_index");

        // call the reset API
        ResetFeatureStateResponse apiResponse = client().execute(ResetFeatureStateAction.INSTANCE, new ResetFeatureStateRequest()).get();
        assertThat(apiResponse.getItemList(), containsInAnyOrder(
            new ResetFeatureStateResponse.ResetFeatureStateStatus("SystemIndexTestPlugin", "SUCCESS"),
            new ResetFeatureStateResponse.ResetFeatureStateStatus("SecondSystemIndexTestPlugin", "SUCCESS"),
            new ResetFeatureStateResponse.ResetFeatureStateStatus("tasks", "SUCCESS")
        ));

        // verify that both indices are gone
        Exception e1 = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex()
            .addIndices(systemIndex1)
            .get());

        assertThat(e1.getMessage(), containsString("no such index"));

        Exception e2 = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex()
            .addIndices(associatedIndex)
            .get());

        assertThat(e2.getMessage(), containsString("no such index"));

        Exception e3 = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex()
            .addIndices(systemIndex2)
            .get());

        assertThat(e3.getMessage(), containsString("no such index"));

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
            return SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin";
        }
    }

    /**
     * A second test plugin with a patterns for system indices.
     */
    public static class SecondSystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_PATTERN = ".second-test-system-idx*";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_PATTERN, "System indices for tests"));
        }

        @Override
        public String getFeatureName() {
            return SecondSystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A second test plugin";
        }
    }
}
