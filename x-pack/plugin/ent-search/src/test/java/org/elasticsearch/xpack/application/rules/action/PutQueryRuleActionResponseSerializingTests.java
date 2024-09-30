/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class PutQueryRuleActionResponseSerializingTests extends AbstractBWCWireSerializationTestCase<PutQueryRuleAction.Response> {

    @Override
    protected Writeable.Reader<PutQueryRuleAction.Response> instanceReader() {
        return PutQueryRuleAction.Response::new;
    }

    @Override
    protected PutQueryRuleAction.Response createTestInstance() {
        return new PutQueryRuleAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected PutQueryRuleAction.Response mutateInstance(PutQueryRuleAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutQueryRuleAction.Response mutateInstanceForVersion(PutQueryRuleAction.Response instance, TransportVersion version) {
        return instance;
    }
}
