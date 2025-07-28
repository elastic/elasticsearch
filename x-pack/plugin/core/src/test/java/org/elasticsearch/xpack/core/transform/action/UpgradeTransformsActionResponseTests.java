/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.UpgradeTransformsAction.Response;

public class UpgradeTransformsActionResponseTests extends AbstractWireSerializingTransformTestCase<Response> {

    public static Response randomUpgradeResponse() {
        return new Response(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return randomUpgradeResponse();
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

}
