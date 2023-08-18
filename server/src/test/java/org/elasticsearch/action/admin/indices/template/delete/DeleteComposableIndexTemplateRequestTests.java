/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeleteComposableIndexTemplateRequestTests extends AbstractWireSerializingTestCase<
    DeleteComposableIndexTemplateAction.Request> {
    @Override
    protected Writeable.Reader<DeleteComposableIndexTemplateAction.Request> instanceReader() {
        return DeleteComposableIndexTemplateAction.Request::new;
    }

    @Override
    protected DeleteComposableIndexTemplateAction.Request createTestInstance() {
        return new DeleteComposableIndexTemplateAction.Request(randomAlphaOfLength(5));
    }

    @Override
    protected DeleteComposableIndexTemplateAction.Request mutateInstance(DeleteComposableIndexTemplateAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
