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
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class PutSynonymRuleActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutSynonymRuleAction.Request> {
    @Override
    protected Writeable.Reader<PutSynonymRuleAction.Request> instanceReader() {
        return PutSynonymRuleAction.Request::new;
    }

    @Override
    protected PutSynonymRuleAction.Request createTestInstance() {
        return new PutSynonymRuleAction.Request(randomIdentifier(), SynonymsTestUtils.randomSynonymRule(), randomBoolean());
    }

    @Override
    protected PutSynonymRuleAction.Request mutateInstance(PutSynonymRuleAction.Request instance) throws IOException {
        String synonymsSetId = instance.synonymsSetId();
        SynonymRule synonymRule = instance.synonymRule();
        boolean refresh = instance.refresh();
        switch (between(0, 2)) {
            case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
            case 1 -> synonymRule = randomValueOtherThan(synonymRule, SynonymsTestUtils::randomSynonymRule);
            case 2 -> refresh = refresh == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PutSynonymRuleAction.Request(synonymsSetId, synonymRule, refresh);
    }
}
