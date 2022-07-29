/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction.Response;

public class PreviewTransformsActionResponseWireTests extends AbstractWireSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return PreviewTransformsActionResponseTests.randomPreviewResponse();
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }

}
