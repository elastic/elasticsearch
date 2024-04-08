/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ClearDeploymentCacheActionResponseTests extends AbstractWireSerializingTestCase<ClearDeploymentCacheAction.Response> {
    @Override
    protected Writeable.Reader<ClearDeploymentCacheAction.Response> instanceReader() {
        return ClearDeploymentCacheAction.Response::new;
    }

    @Override
    protected ClearDeploymentCacheAction.Response createTestInstance() {
        return new ClearDeploymentCacheAction.Response(randomBoolean());
    }

    @Override
    protected ClearDeploymentCacheAction.Response mutateInstance(ClearDeploymentCacheAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
