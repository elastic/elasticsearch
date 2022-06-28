/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState.FeatureAware;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;

import static org.elasticsearch.test.VersionUtils.randomVersionBetween;

public class FeatureAwareTests extends ESTestCase {

    abstract static class Custom implements Metadata.Custom {

        private final Version version;

        Custom(final Version version) {
            this.version = version;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.ALL_CONTEXTS;
        }

        @Override
        public Diff<Metadata.Custom> diff(final Metadata.Custom previousState) {
            return null;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return builder;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return version;
        }

    }

    static class NoRequiredFeatureCustom extends Custom {

        NoRequiredFeatureCustom(final Version version) {
            super(version);
        }

        @Override
        public String getWriteableName() {
            return "no-required-feature";
        }

    }

    static class RequiredFeatureCustom extends Custom {

        RequiredFeatureCustom(final Version version) {
            super(version);
        }

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public Optional<String> getRequiredFeature() {
            return Optional.of("required-feature");
        }

    }

    public void testVersion() {
        final Version version = randomValueOtherThan(VersionUtils.getFirstVersion(), () -> VersionUtils.randomVersion(random()));
        for (final Custom custom : Arrays.asList(new NoRequiredFeatureCustom(version), new RequiredFeatureCustom(version))) {
            {
                final BytesStreamOutput out = new BytesStreamOutput();
                final Version afterVersion = randomVersionBetween(random(), version, Version.CURRENT);
                out.setVersion(afterVersion);
                if (custom.getRequiredFeature().isPresent()) {
                    out.setFeatures(Collections.singleton(custom.getRequiredFeature().get()));
                }
                assertTrue(FeatureAware.shouldSerialize(out, custom));
            }
            {
                final BytesStreamOutput out = new BytesStreamOutput();
                final Version beforeVersion = randomVersionBetween(
                    random(),
                    VersionUtils.getFirstVersion(),
                    VersionUtils.getPreviousVersion(version)
                );
                out.setVersion(beforeVersion);
                if (custom.getRequiredFeature().isPresent() && randomBoolean()) {
                    out.setFeatures(Collections.singleton(custom.getRequiredFeature().get()));
                }
                assertFalse(FeatureAware.shouldSerialize(out, custom));
            }
        }
    }

    public void testFeature() {
        final Version version = VersionUtils.randomVersion(random());
        final Version afterVersion = randomVersionBetween(random(), version, Version.CURRENT);
        final Custom custom = new RequiredFeatureCustom(version);
        {
            // the feature is present and the client is not a transport client
            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(afterVersion);
            assertTrue(custom.getRequiredFeature().isPresent());
            out.setFeatures(Collections.singleton(custom.getRequiredFeature().get()));
            assertTrue(FeatureAware.shouldSerialize(out, custom));
        }
        {
            // the feature is present and the client is a transport client
            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(afterVersion);
            assertTrue(custom.getRequiredFeature().isPresent());
            out.setFeatures(new HashSet<>(Arrays.asList(custom.getRequiredFeature().get(), TransportClient.TRANSPORT_CLIENT_FEATURE)));
            assertTrue(FeatureAware.shouldSerialize(out, custom));
        }
    }

    public void testMissingFeature() {
        final Version version = VersionUtils.randomVersion(random());
        final Version afterVersion = randomVersionBetween(random(), version, Version.CURRENT);
        final Custom custom = new RequiredFeatureCustom(version);
        {
            // the feature is missing but we should serialize it anyway because the client is not a transport client
            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(afterVersion);
            assertTrue(FeatureAware.shouldSerialize(out, custom));
        }
        {
            // the feature is missing and we should not serialize it because the client is a transport client
            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(afterVersion);
            out.setFeatures(Collections.singleton(TransportClient.TRANSPORT_CLIENT_FEATURE));
            assertFalse(FeatureAware.shouldSerialize(out, custom));
        }
    }

}
