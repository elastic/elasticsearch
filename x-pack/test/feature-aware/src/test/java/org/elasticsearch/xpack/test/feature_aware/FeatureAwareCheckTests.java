/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.test.feature_aware;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class FeatureAwareCheckTests extends ESTestCase {

    public void testClusterStateCustomViolation() throws IOException {
        runCustomViolationTest(
                ClusterStateCustomViolation.class,
                getClass(),
                ClusterState.Custom.class,
                XPackPlugin.XPackClusterStateCustom.class);
    }

    public void testClusterStateCustom() throws IOException {
        runCustomTest(XPackClusterStateCustom.class, getClass(), ClusterState.Custom.class, XPackPlugin.XPackClusterStateCustom.class);
    }

    public void testClusterStateCustomMarkerInterface() throws IOException {
        // marker interfaces do not implement the marker interface but should not fail the feature aware check
        runCustomTest(
                XPackPlugin.XPackClusterStateCustom.class,
                XPackPlugin.class,
                ClusterState.Custom.class,
                XPackPlugin.XPackClusterStateCustom.class);
    }

    public void testMetaDataCustomViolation() throws IOException {
        runCustomViolationTest(MetaDataCustomViolation.class, getClass(), MetaData.Custom.class, XPackPlugin.XPackMetaDataCustom.class);
    }

    public void testMetaDataCustom() throws IOException {
        runCustomTest(XPackMetaDataCustom.class, getClass(), MetaData.Custom.class, XPackPlugin.XPackMetaDataCustom.class);
    }

    public void testMetaDataCustomMarkerInterface() throws IOException {
        // marker interfaces do not implement the marker interface but should not fail the feature aware check
        runCustomTest(
                XPackPlugin.XPackMetaDataCustom.class,
                XPackPlugin.class,
                MetaData.Custom.class,
                XPackPlugin.XPackMetaDataCustom.class);
    }

    public void testPersistentTaskParamsViolation() throws IOException {
        runCustomViolationTest(
                PersistentTaskParamsViolation.class,
                getClass(),
                PersistentTaskParams.class,
                XPackPlugin.XPackPersistentTaskParams.class);
    }

    public void testPersistentTaskParams() throws IOException {
        runCustomTest(XPackPersistentTaskParams.class, getClass(), PersistentTaskParams.class, XPackPlugin.XPackPersistentTaskParams.class);
    }

    public void testPersistentTaskParamsMarkerInterface() throws IOException {
        // marker interfaces do not implement the marker interface but should not fail the feature aware check
        runCustomTest(
                XPackPlugin.XPackPersistentTaskParams.class,
                XPackPlugin.class,
                PersistentTaskParams.class,
                XPackPlugin.XPackPersistentTaskParams.class);
    }

    abstract class ClusterStateCustomFeatureAware implements ClusterState.Custom {

        private final String writeableName;

        ClusterStateCustomFeatureAware(final String writeableName) {
            this.writeableName = writeableName;
        }

        @Override
        public Diff<ClusterState.Custom> diff(ClusterState.Custom previousState) {
            return null;
        }

        @Override
        public String getWriteableName() {
            return writeableName;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return builder;
        }

    }

    class ClusterStateCustomViolation extends ClusterStateCustomFeatureAware {

        ClusterStateCustomViolation() {
            super("cluster_state_custom_violation");
        }
    }

    class XPackClusterStateCustom extends ClusterStateCustomFeatureAware implements XPackPlugin.XPackClusterStateCustom {

        XPackClusterStateCustom() {
            super("x_pack_cluster_state_custom");
        }

    }

    abstract class MetaDataCustomFeatureAware implements MetaData.Custom {

        private final String writeableName;

        MetaDataCustomFeatureAware(final String writeableName) {
            this.writeableName = writeableName;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return MetaData.ALL_CONTEXTS;
        }

        @Override
        public Diff<MetaData.Custom> diff(MetaData.Custom previousState) {
            return null;
        }

        @Override
        public String getWriteableName() {
            return writeableName;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return builder;
        }

    }

    class MetaDataCustomViolation extends MetaDataCustomFeatureAware {

        MetaDataCustomViolation() {
            super("meta_data_custom_violation");
        }

    }

    class XPackMetaDataCustom extends MetaDataCustomFeatureAware implements XPackPlugin.XPackMetaDataCustom {

        XPackMetaDataCustom() {
            super("x_pack_meta_data_custom");
        }

    }

    abstract class PersistentTaskParamsFeatureAware implements PersistentTaskParams {

        private final String writeableName;

        PersistentTaskParamsFeatureAware(final String writeableName) {
            this.writeableName = writeableName;
        }

        @Override
        public String getWriteableName() {
            return writeableName;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return builder;
        }

    }

    class PersistentTaskParamsViolation extends PersistentTaskParamsFeatureAware {

        PersistentTaskParamsViolation() {
            super("persistent_task_params_violation");
        }

    }

    class XPackPersistentTaskParams extends PersistentTaskParamsFeatureAware implements XPackPlugin.XPackPersistentTaskParams {

        XPackPersistentTaskParams() {
            super("x_pack_persistent_task_params");
        }

    }

    private class FeatureAwareViolationConsumer implements Consumer<FeatureAwareCheck.FeatureAwareViolation> {

        private final AtomicBoolean called = new AtomicBoolean();
        private final String name;
        private final String interfaceName;
        private final String expectedInterfaceName;

        FeatureAwareViolationConsumer(final String name, final String interfaceName, final String expectedInterfaceName) {
            this.name = name;
            this.interfaceName = interfaceName;
            this.expectedInterfaceName = expectedInterfaceName;
        }

        @Override
        public void accept(final org.elasticsearch.xpack.test.feature_aware.FeatureAwareCheck.FeatureAwareViolation featureAwareViolation) {
            called.set(true);
            assertThat(featureAwareViolation.name, equalTo(name));
            assertThat(featureAwareViolation.interfaceName, equalTo(interfaceName));
            assertThat(featureAwareViolation.expectedInterfaceName, equalTo(expectedInterfaceName));
        }

    }

    /**
     * Runs a test on an actual class implementing a custom interface and not the expected marker interface.
     *
     * @param clazz                  the custom implementation
     * @param outerClazz             the outer class to load the custom implementation relative to
     * @param interfaceClazz         the custom
     * @param expectedInterfaceClazz the marker interface
     * @throws IOException if an I/O error occurs reading the class
     */
    private void runCustomViolationTest(
            final Class<? extends ClusterState.FeatureAware> clazz,
            final Class<?> outerClazz,
            final Class<? extends ClusterState.FeatureAware> interfaceClazz,
            final Class<? extends ClusterState.FeatureAware> expectedInterfaceClazz) throws IOException {
        runTest(clazz, outerClazz, interfaceClazz, expectedInterfaceClazz, true);
    }

    /**
     * Runs a test on an actual class implementing a custom interface and the expected marker interface.
     *
     * @param clazz                  the custom implementation
     * @param outerClazz             the outer class to load the custom implementation relative to
     * @param interfaceClazz         the custom
     * @param expectedInterfaceClazz the marker interface
     * @throws IOException if an I/O error occurs reading the class
     */
    private void runCustomTest(
            final Class<? extends ClusterState.FeatureAware> clazz,
            final Class<?> outerClazz,
            final Class<? extends ClusterState.FeatureAware> interfaceClazz,
            final Class<? extends ClusterState.FeatureAware> expectedInterfaceClazz) throws IOException {
        runTest(clazz, outerClazz, interfaceClazz, expectedInterfaceClazz, false);
    }

    /**
     * Runs a test on an actual class implementing a custom interface and should implement the expected marker interface if and only if
     * the specified violation parameter is false.
     *
     * @param clazz                  the custom implementation
     * @param outerClazz             the outer class to load the custom implementation relative to
     * @param interfaceClazz         the custom
     * @param expectedInterfaceClazz the marker interface
     * @param violation              whether or not the actual class is expected to fail the feature aware check
     * @throws IOException if an I/O error occurs reading the class
     */
    private void runTest(
            final Class<? extends ClusterState.FeatureAware> clazz,
            final Class<?> outerClazz,
            final Class<? extends ClusterState.FeatureAware> interfaceClazz,
            final Class<? extends ClusterState.FeatureAware> expectedInterfaceClazz,
            final boolean violation) throws IOException {
        final String name = clazz.getName();
        final FeatureAwareViolationConsumer callback =
                new FeatureAwareViolationConsumer(
                        FeatureAwareCheck.formatClassName(clazz),
                        FeatureAwareCheck.formatClassName(interfaceClazz),
                        FeatureAwareCheck.formatClassName(expectedInterfaceClazz));
        FeatureAwareCheck.checkClass(outerClazz.getResourceAsStream(name.substring(1 + name.lastIndexOf(".")) + ".class"), callback);
        assertThat(callback.called.get(), equalTo(violation));
    }

}
