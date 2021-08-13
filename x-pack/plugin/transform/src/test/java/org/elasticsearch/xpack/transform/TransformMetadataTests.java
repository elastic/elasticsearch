/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.transform.TransformMetadata;

public class TransformMetadataTests extends AbstractSerializingTestCase<TransformMetadata> {

    @Override
    protected TransformMetadata createTestInstance() {
        return new TransformMetadata.Builder().isResetMode(randomBoolean()).build();
    }

    @Override
    protected Writeable.Reader<TransformMetadata> instanceReader() {
        return TransformMetadata::new;
    }

    @Override
    protected TransformMetadata doParseInstance(XContentParser parser) {
        return TransformMetadata.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected TransformMetadata mutateInstance(TransformMetadata instance) {
        return new TransformMetadata.Builder().isResetMode(instance.isResetMode() == false).build();
    }
}
