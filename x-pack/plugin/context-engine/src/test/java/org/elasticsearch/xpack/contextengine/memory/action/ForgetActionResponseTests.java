/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ForgetActionResponseTests extends AbstractWireSerializingTestCase<ForgetAction.Response> {

    @Override
    protected Writeable.Reader<ForgetAction.Response> instanceReader() {
        return ForgetAction.Response::new;
    }

    @Override
    protected ForgetAction.Response createTestInstance() {
        return new ForgetAction.Response(randomIntBetween(0, 1000));
    }

    @Override
    protected ForgetAction.Response mutateInstance(ForgetAction.Response instance) {
        return new ForgetAction.Response(randomValueOtherThan(instance.deleted(), () -> randomIntBetween(0, 1000)));
    }
}
