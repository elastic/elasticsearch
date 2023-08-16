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

public class PutQueryRulesetActionResponseSerializingTests extends AbstractBWCWireSerializationTestCase<PutQueryRulesetAction.Response> {

    @Override
    protected Writeable.Reader<PutQueryRulesetAction.Response> instanceReader() {
        return PutQueryRulesetAction.Response::new;
    }

    @Override
    protected PutQueryRulesetAction.Response createTestInstance() {
        return new PutQueryRulesetAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected PutQueryRulesetAction.Response mutateInstance(PutQueryRulesetAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutQueryRulesetAction.Response mutateInstanceForVersion(PutQueryRulesetAction.Response instance, TransportVersion version) {
        return instance;
    }
}
