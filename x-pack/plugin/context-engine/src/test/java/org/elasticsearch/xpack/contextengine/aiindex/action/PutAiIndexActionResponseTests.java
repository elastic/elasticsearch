/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndexTestUtils;

public class PutAiIndexActionResponseTests extends AbstractWireSerializingTestCase<PutAiIndexAction.Response> {

    @Override
    protected Writeable.Reader<PutAiIndexAction.Response> instanceReader() {
        return PutAiIndexAction.Response::new;
    }

    @Override
    protected PutAiIndexAction.Response createTestInstance() {
        return new PutAiIndexAction.Response(
            AiIndexTestUtils.randomAiIndex(),
            randomFrom(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED)
        );
    }

    @Override
    protected PutAiIndexAction.Response mutateInstance(PutAiIndexAction.Response instance) {
        return new PutAiIndexAction.Response(randomValueOtherThan(instance.aiIndex(), AiIndexTestUtils::randomAiIndex), instance.result());
    }
}
