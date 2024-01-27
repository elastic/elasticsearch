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

public class DeleteSynonymRuleActionRequestSerializingTests extends AbstractWireSerializingTestCase<DeleteSynonymRuleAction.Request> {

    @Override
    protected Writeable.Reader<DeleteSynonymRuleAction.Request> instanceReader() {
        return DeleteSynonymRuleAction.Request::new;
    }

    @Override
    protected DeleteSynonymRuleAction.Request createTestInstance() {
        return new DeleteSynonymRuleAction.Request(randomIdentifier(), randomIdentifier());
    }

    @Override
    protected DeleteSynonymRuleAction.Request mutateInstance(DeleteSynonymRuleAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
