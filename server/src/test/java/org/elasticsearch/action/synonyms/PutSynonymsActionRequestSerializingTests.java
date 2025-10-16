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
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;

public class PutSynonymsActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutSynonymsAction.Request> {

    @Override
    protected Writeable.Reader<PutSynonymsAction.Request> instanceReader() {
        return PutSynonymsAction.Request::new;
    }

    @Override
    protected PutSynonymsAction.Request createTestInstance() {
        return new PutSynonymsAction.Request(randomIdentifier(), randomSynonymsSet(), randomBoolean());
    }

    @Override
    protected PutSynonymsAction.Request mutateInstance(PutSynonymsAction.Request instance) throws IOException {
        String synonymsSetId = instance.synonymsSetId();
        List<SynonymRule> synonymRules = Arrays.asList(instance.synonymRules());
        boolean refresh = instance.refresh();
        switch (between(0, 2)) {
            case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
            case 1 -> synonymRules = randomValueOtherThan(synonymRules, () -> Arrays.asList(randomSynonymsSet()));
            case 2 -> refresh = refresh == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PutSynonymsAction.Request(synonymsSetId, synonymRules.toArray(new SynonymRule[0]), refresh);
    }
}
