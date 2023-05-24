/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsSet;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class PutSynonymsActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutSynonymsAction.Request> {

    private static SynonymsSet randomSynonymsset() {
        return new SynonymsSet(randomArray(10, SynonymRule[]::new, PutSynonymsActionRequestSerializingTests::randomSynonymRule));
    }

    private static SynonymRule randomSynonymRule() {
        return new SynonymRule(
            randomBoolean() ? null : randomIdentifier(),
            Arrays.stream(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(1, 10))).collect(Collectors.joining(", "))
        );
    }

    @Override
    protected Writeable.Reader<PutSynonymsAction.Request> instanceReader() {
        return PutSynonymsAction.Request::new;
    }

    @Override
    protected PutSynonymsAction.Request createTestInstance() {
        return new PutSynonymsAction.Request(randomIdentifier(), randomSynonymsset());
    }

    @Override
    protected PutSynonymsAction.Request mutateInstance(PutSynonymsAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
