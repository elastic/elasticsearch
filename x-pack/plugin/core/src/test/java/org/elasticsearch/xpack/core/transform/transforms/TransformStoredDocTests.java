/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Collections;

public class TransformStoredDocTests extends AbstractSerializingTransformTestCase<TransformStoredDoc> {

    protected static ToXContent.Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(TransformField.FOR_INTERNAL_STORAGE, "true")
    );

    public static TransformStoredDoc randomTransformStoredDoc(String id) {
        return new TransformStoredDoc(id, TransformStateTests.randomTransformState(), TransformIndexerStatsTests.randomStats());
    }

    public static TransformStoredDoc randomTransformStoredDoc() {
        return randomTransformStoredDoc(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected TransformStoredDoc doParseInstance(XContentParser parser) throws IOException {
        return TransformStoredDoc.PARSER.apply(parser, null);
    }

    @Override
    // Setting params for internal storage so that we can check XContent equivalence as
    // TransformIndexerTransformStats does not write the ID to the XContentObject unless it is for internal storage
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    @Override
    protected TransformStoredDoc createTestInstance() {
        return randomTransformStoredDoc();
    }

    @Override
    protected Reader<TransformStoredDoc> instanceReader() {
        return TransformStoredDoc::new;
    }
}
