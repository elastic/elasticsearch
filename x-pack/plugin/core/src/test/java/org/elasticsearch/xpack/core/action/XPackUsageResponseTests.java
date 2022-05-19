/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class XPackUsageResponseTests extends ESTestCase {

    private static Version oldVersion;
    private static Version newVersion;

    @BeforeClass
    public static void setVersion() {
        oldVersion = VersionUtils.randomVersionBetween(
            random(),
            VersionUtils.getFirstVersion(),
            VersionUtils.getPreviousVersion(VersionUtils.getPreviousMinorVersion())
        );
        newVersion = VersionUtils.randomVersionBetween(random(), VersionUtils.getPreviousMinorVersion(), Version.CURRENT);
    }

    public static class OldUsage extends XPackFeatureSet.Usage {

        public OldUsage() {
            super("old", randomBoolean(), randomBoolean());
        }

        public OldUsage(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return oldVersion;
        }

    }

    public static class NewUsage extends XPackFeatureSet.Usage {

        public NewUsage() {
            super("new", randomBoolean(), randomBoolean());
        }

        public NewUsage(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return newVersion;
        }

    }

    public void testVersionDependentSerializationWriteToOldStream() throws IOException {
        final XPackUsageResponse before = new XPackUsageResponse(List.of(new OldUsage(), new NewUsage()));
        final BytesStreamOutput oldStream = new BytesStreamOutput();
        oldStream.setVersion(VersionUtils.randomVersionBetween(random(), oldVersion, VersionUtils.getPreviousVersion(newVersion)));
        before.writeTo(oldStream);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, "old", OldUsage::new),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, "new", NewUsage::new)
            )
        );

        final StreamInput in = new NamedWriteableAwareStreamInput(oldStream.bytes().streamInput(), registry);
        final XPackUsageResponse after = new XPackUsageResponse(in);
        assertThat(after.getUsages(), hasSize(1));
        assertThat(after.getUsages().get(0), instanceOf(OldUsage.class));
    }

    public void testVersionDependentSerializationWriteToNewStream() throws IOException {
        final XPackUsageResponse before = new XPackUsageResponse(List.of(new OldUsage(), new NewUsage()));
        final BytesStreamOutput newStream = new BytesStreamOutput();
        newStream.setVersion(VersionUtils.randomVersionBetween(random(), newVersion, Version.CURRENT));
        before.writeTo(newStream);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, "old", OldUsage::new),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, "new", NewUsage::new)
            )
        );

        final StreamInput in = new NamedWriteableAwareStreamInput(newStream.bytes().streamInput(), registry);
        final XPackUsageResponse after = new XPackUsageResponse(in);
        assertThat(after.getUsages(), hasSize(2));
        assertThat(after.getUsages().get(0), instanceOf(OldUsage.class));
        assertThat(after.getUsages().get(1), instanceOf(NewUsage.class));
    }

}
