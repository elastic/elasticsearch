/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class LifecycleOperationMetadataTests extends AbstractChunkedSerializingTestCase<Metadata.ProjectCustom> {

    @Override
    protected LifecycleOperationMetadata createTestInstance() {
        return new LifecycleOperationMetadata(randomFrom(OperationMode.values()), randomFrom(OperationMode.values()));
    }

    @Override
    protected LifecycleOperationMetadata doParseInstance(XContentParser parser) throws IOException {
        return LifecycleOperationMetadata.PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<Metadata.ProjectCustom> instanceReader() {
        return LifecycleOperationMetadata::new;
    }

    @Override
    protected Metadata.ProjectCustom mutateInstance(Metadata.ProjectCustom instance) {
        LifecycleOperationMetadata metadata = (LifecycleOperationMetadata) instance;
        if (randomBoolean()) {
            return new LifecycleOperationMetadata(
                randomValueOtherThan(metadata.getILMOperationMode(), () -> randomFrom(OperationMode.values())),
                metadata.getSLMOperationMode()
            );
        } else {
            return new LifecycleOperationMetadata(
                metadata.getILMOperationMode(),
                randomValueOtherThan(metadata.getSLMOperationMode(), () -> randomFrom(OperationMode.values()))
            );
        }
    }

    public void testMinimumSupportedVersion() {
        TransportVersion min = createTestInstance().getMinimalSupportedVersion();
        assertTrue(
            min.onOrBefore(TransportVersionUtils.randomVersionBetween(random(), TransportVersions.V_8_7_0, TransportVersion.current()))
        );
    }

    public void testcontext() {
        assertThat(createTestInstance().context(), containsInAnyOrder(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY));
    }
}
