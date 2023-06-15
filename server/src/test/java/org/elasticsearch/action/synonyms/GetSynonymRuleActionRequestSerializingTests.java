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

public class GetSynonymRuleActionRequestSerializingTests extends AbstractWireSerializingTestCase<GetSynonymRuleAction.Request> {

    @Override
    protected Writeable.Reader<GetSynonymRuleAction.Request> instanceReader() {
        return GetSynonymRuleAction.Request::new;
    }

    @Override
    protected GetSynonymRuleAction.Request createTestInstance() {
        return new GetSynonymRuleAction.Request(
            randomIdentifier(),
            randomIdentifier()
        );
    }

    @Override
    protected GetSynonymRuleAction.Request mutateInstance(GetSynonymRuleAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
