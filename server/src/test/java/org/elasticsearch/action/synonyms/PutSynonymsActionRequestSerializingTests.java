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
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;

public class PutSynonymsActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutSynonymsAction.Request> {

    @Override
    protected Writeable.Reader<PutSynonymsAction.Request> instanceReader() {
        return PutSynonymsAction.Request::new;
    }

    @Override
    protected PutSynonymsAction.Request createTestInstance() {
        return new PutSynonymsAction.Request(randomIdentifier(), randomSynonymsSet(), randomBoolean(), randomBoolean());
    }

    @Override
    protected PutSynonymsAction.Request mutateInstance(PutSynonymsAction.Request instance) throws IOException {
        String synonymsSetId = instance.synonymsSetId();
        SynonymRule[] synonymRules = instance.synonymRules();
        boolean refresh = instance.refresh();
        boolean append = instance.append();
        switch (between(0, 3)) {
            case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
            case 1 -> synonymRules = randomArrayOtherThan(synonymRules, () -> randomSynonymsSet());
            case 2 -> refresh = refresh == false;
            case 3 -> append = append == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PutSynonymsAction.Request(synonymsSetId, synonymRules, refresh, append);
    }

    /**
     * Serializing append=true to a node that does not support it must throw rather than silently
     * dropping the flag and defaulting to replace behaviour.
     */
    public void testWriteToThrowsWhenAppendTrueAndVersionNotSupporting() throws IOException {
        var oldVersion = TransportVersionUtils.randomVersionNotSupporting(PutSynonymsAction.Request.SYNONYMS_APPEND_PARAM);
        PutSynonymsAction.Request request = new PutSynonymsAction.Request(randomIdentifier(), randomSynonymsSet(), randomBoolean(), true);
        expectThrows(IllegalArgumentException.class, () -> copyInstance(request, oldVersion));
    }
}
