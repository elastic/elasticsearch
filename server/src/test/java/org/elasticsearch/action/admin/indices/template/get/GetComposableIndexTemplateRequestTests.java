/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetComposableIndexTemplateRequestTests extends AbstractWireSerializingTestCase<GetComposableIndexTemplateAction.Request> {
    @Override
    protected Writeable.Reader<GetComposableIndexTemplateAction.Request> instanceReader() {
        return GetComposableIndexTemplateAction.Request::new;
    }

    @Override
    protected GetComposableIndexTemplateAction.Request createTestInstance() {
        return new GetComposableIndexTemplateAction.Request(randomBoolean() ? null : randomAlphaOfLength(4));
    }

    @Override
    protected GetComposableIndexTemplateAction.Request mutateInstance(GetComposableIndexTemplateAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
