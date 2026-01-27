/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
        return new DeleteSynonymRuleAction.Request(randomIdentifier(), randomIdentifier(), randomBoolean());
    }

    @Override
    protected DeleteSynonymRuleAction.Request mutateInstance(DeleteSynonymRuleAction.Request instance) throws IOException {
        String synonymsSetId = instance.synonymsSetId();
        String synonymRuleId = instance.synonymRuleId();
        boolean refresh = instance.refresh();
        switch (between(0, 2)) {
            case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
            case 1 -> synonymRuleId = randomValueOtherThan(synonymRuleId, () -> randomIdentifier());
            case 2 -> refresh = refresh == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DeleteSynonymRuleAction.Request(synonymsSetId, synonymRuleId, refresh);
    }
}
