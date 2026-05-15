/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

import java.io.IOException;

public class ClearOAuth2TokenCacheActionTests extends AbstractBWCWireSerializationTestCase<
    ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage> {

    @Override
    protected Writeable.Reader<ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage> instanceReader() {
        return ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage::new;
    }

    @Override
    protected ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage createTestInstance() {
        return new ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage(randomKey());
    }

    @Override
    protected ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage mutateInstance(
        ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage instance
    ) throws IOException {
        return new ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage(
            new InferenceIdAndProject(instance.key().inferenceEntityId() + randomAlphaOfLength(3), instance.key().projectId())
        );
    }

    private static InferenceIdAndProject randomKey() {
        var projectId = randomBoolean() ? ProjectId.DEFAULT : ProjectId.fromId(randomAlphaOfLength(8));
        return new InferenceIdAndProject(randomAlphaOfLength(10), projectId);
    }

    @Override
    protected ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage mutateInstanceForVersion(
        ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage instance,
        TransportVersion version
    ) {
        return instance;
    }
}
