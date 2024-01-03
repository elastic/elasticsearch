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

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;

public class PutSynonymsActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutSynonymsAction.Request> {

    @Override
    protected Writeable.Reader<PutSynonymsAction.Request> instanceReader() {
        return PutSynonymsAction.Request::new;
    }

    @Override
    protected PutSynonymsAction.Request createTestInstance() {
        return new PutSynonymsAction.Request(randomIdentifier(), randomSynonymsSet());
    }

    @Override
    protected PutSynonymsAction.Request mutateInstance(PutSynonymsAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
