/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeleteComposableIndexTemplateRequestTests extends AbstractWireSerializingTestCase<
    TransportDeleteComposableIndexTemplateAction.Request> {
    @Override
    protected Writeable.Reader<TransportDeleteComposableIndexTemplateAction.Request> instanceReader() {
        return TransportDeleteComposableIndexTemplateAction.Request::new;
    }

    @Override
    protected TransportDeleteComposableIndexTemplateAction.Request createTestInstance() {
        return new TransportDeleteComposableIndexTemplateAction.Request(randomAlphaOfLength(5));
    }

    @Override
    protected TransportDeleteComposableIndexTemplateAction.Request mutateInstance(
        TransportDeleteComposableIndexTemplateAction.Request instance
    ) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
