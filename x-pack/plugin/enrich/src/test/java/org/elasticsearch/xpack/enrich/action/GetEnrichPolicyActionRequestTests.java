/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;

public class GetEnrichPolicyActionRequestTests extends AbstractWireSerializingTestCase<GetEnrichPolicyAction.Request> {

    @Override
    protected GetEnrichPolicyAction.Request createTestInstance() {
        return new GetEnrichPolicyAction.Request(generateRandomStringArray(0, 4, false));
    }

    @Override
    protected Writeable.Reader<GetEnrichPolicyAction.Request> instanceReader() {
        return GetEnrichPolicyAction.Request::new;
    }
}
