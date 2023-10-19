/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class XPackUsageResponseTests extends ESTestCase {

    private static TransportVersion oldVersion;
    private static TransportVersion newVersion;

    @BeforeClass
    public static void setVersion() {
        oldVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersionUtils.getFirstVersion(),
            TransportVersionUtils.getPreviousVersion(TransportVersion.current())
        );
        newVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersionUtils.getNextVersion(oldVersion),
            TransportVersion.current()
        );
    }

    public static class OldUsage extends XPackFeatureSet.Usage {

        public OldUsage() {
            super("old", randomBoolean(), randomBoolean());
        }

        public OldUsage(final StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
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
        public TransportVersion getMinimalSupportedVersion() {
            return newVersion;
        }

    }

    public void testVersionDependentSerializationWriteToOldStream() throws IOException {
        final XPackUsageResponse before = new XPackUsageResponse(List.of(new OldUsage(), new NewUsage()));
        final BytesStreamOutput oldStream = new BytesStreamOutput();
        oldStream.setTransportVersion(oldVersion);
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
        newStream.setTransportVersion(newVersion);
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
