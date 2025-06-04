/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.AssociatedIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class FeatureStateResetApiIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SystemIndexTestPlugin.class);
        plugins.add(SecondSystemIndexTestPlugin.class);
        plugins.add(EvilSystemIndexTestPlugin.class);
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
        ResetFeatureStateResponse apiResponse = client().execute(
            ResetFeatureStateAction.INSTANCE,
            new ResetFeatureStateRequest(TEST_REQUEST_TIMEOUT)
        ).get();
        assertThat(
            apiResponse.getFeatureStateResetStatuses(),
            containsInAnyOrder(
                ResetFeatureStateResponse.ResetFeatureStateStatus.success("SystemIndexTestPlugin"),
                ResetFeatureStateResponse.ResetFeatureStateStatus.success("SecondSystemIndexTestPlugin"),
                ResetFeatureStateResponse.ResetFeatureStateStatus.success("EvilSystemIndexTestPlugin"),
                ResetFeatureStateResponse.ResetFeatureStateStatus.success("tasks"),
                ResetFeatureStateResponse.ResetFeatureStateStatus.success("synonyms")
            )
        );

        // verify that both indices are gone
        Exception e1 = expectThrows(
            IndexNotFoundException.class,
            indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(systemIndex1)
        );
        assertThat(e1.getMessage(), containsString("no such index"));

        Exception e2 = expectThrows(
            IndexNotFoundException.class,
            indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(associatedIndex)
        );
        assertThat(e2.getMessage(), containsString("no such index"));

        Exception e3 = expectThrows(
            IndexNotFoundException.class,
            indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(systemIndex2)
        );
        assertThat(e3.getMessage(), containsString("no such index"));

        GetIndexResponse response = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices("my_index").get();
        assertThat(response.getIndices(), arrayContaining("my_index"));
    }

    /**
     * Evil test - test that when a feature fails to reset, we get a response object
     * indicating the failure
     */
    public void testFeatureResetFailure() throws Exception {
        try {
            EvilSystemIndexTestPlugin.setBeEvil(true);
            ResetFeatureStateResponse resetFeatureStateResponse = client().execute(
                ResetFeatureStateAction.INSTANCE,
                new ResetFeatureStateRequest(TEST_REQUEST_TIMEOUT)
            ).get();

            List<String> failedFeatures = resetFeatureStateResponse.getFeatureStateResetStatuses()
                .stream()
                .filter(status -> status.getStatus() == ResetFeatureStateResponse.ResetFeatureStateStatus.Status.FAILURE)
                .peek(status -> assertThat(status.getException(), notNullValue()))
                .map(status -> {
                    // all failed statuses should have exceptions
                    assertThat(status.getException(), notNullValue());
                    return status.getFeatureName();
                })
                .toList();
            assertThat(failedFeatures, contains("EvilSystemIndexTestPlugin"));
        } finally {
            EvilSystemIndexTestPlugin.setBeEvil(false);
        }
    }

    /**
     * A test plugin with patterns for system indices and associated indices.
     */
    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_PATTERN = ".test-system-idx*";
        public static final String ASSOCIATED_INDEX_PATTERN = ".associated-idx*";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(SystemIndexDescriptorUtils.createUnmanaged(SYSTEM_INDEX_PATTERN, "System indices for tests"));
        }

        @Override
        public Collection<AssociatedIndexDescriptor> getAssociatedIndexDescriptors() {
            return Collections.singletonList(new AssociatedIndexDescriptor(ASSOCIATED_INDEX_PATTERN, "Associated indices"));
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
            return Collections.singletonList(SystemIndexDescriptorUtils.createUnmanaged(SYSTEM_INDEX_PATTERN, "System indices for tests"));
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

    /**
     * An evil test plugin to test failure cases.
     */
    public static class EvilSystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        private static boolean beEvil = false;

        @Override
        public String getFeatureName() {
            return "EvilSystemIndexTestPlugin";
        }

        @Override
        public String getFeatureDescription() {
            return "a plugin that can be very bad";
        }

        public static synchronized void setBeEvil(boolean evil) {
            beEvil = evil;
        }

        public static synchronized boolean isEvil() {
            return beEvil;
        }

        @Override
        public void cleanUpFeature(
            ClusterService clusterService,
            ProjectResolver projectResolver,
            Client client,
            ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> listener
        ) {
            if (isEvil()) {
                listener.onResponse(
                    ResetFeatureStateResponse.ResetFeatureStateStatus.failure(getFeatureName(), new ElasticsearchException("problem!"))
                );
            } else {
                listener.onResponse(ResetFeatureStateResponse.ResetFeatureStateStatus.success(getFeatureName()));
            }
        }
    }
}
