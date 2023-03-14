/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;

public class DeleteEnrichPolicyActionRequestTests extends AbstractWireSerializingTestCase<DeleteEnrichPolicyAction.Request> {
    @Override
    protected DeleteEnrichPolicyAction.Request createTestInstance() {
        return new DeleteEnrichPolicyAction.Request(randomAlphaOfLength(4));
    }

    @Override
    protected DeleteEnrichPolicyAction.Request mutateInstance(DeleteEnrichPolicyAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<DeleteEnrichPolicyAction.Request> instanceReader() {
        return DeleteEnrichPolicyAction.Request::new;
    }
}
