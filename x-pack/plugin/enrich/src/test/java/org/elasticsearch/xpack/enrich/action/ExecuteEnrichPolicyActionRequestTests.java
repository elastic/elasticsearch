/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;

public class ExecuteEnrichPolicyActionRequestTests extends AbstractWireSerializingTestCase<ExecuteEnrichPolicyAction.Request> {

    @Override
    protected ExecuteEnrichPolicyAction.Request createTestInstance() {
        return new ExecuteEnrichPolicyAction.Request(randomAlphaOfLength(3));
    }

    @Override
    protected Writeable.Reader<ExecuteEnrichPolicyAction.Request> instanceReader() {
        return ExecuteEnrichPolicyAction.Request::new;
    }
}
