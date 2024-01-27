/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class GetSynonymsSetsActionRequestSerializingTests extends AbstractWireSerializingTestCase<GetSynonymsSetsAction.Request> {

    @Override
    protected Writeable.Reader<GetSynonymsSetsAction.Request> instanceReader() {
        return GetSynonymsSetsAction.Request::new;
    }

    @Override
    protected GetSynonymsSetsAction.Request createTestInstance() {
        return new GetSynonymsSetsAction.Request(randomIntBetween(0, Integer.MAX_VALUE), randomIntBetween(0, Integer.MAX_VALUE));
    }

    @Override
    protected GetSynonymsSetsAction.Request mutateInstance(GetSynonymsSetsAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
