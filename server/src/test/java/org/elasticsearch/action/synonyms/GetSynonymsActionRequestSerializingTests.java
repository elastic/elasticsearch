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

public class GetSynonymsActionRequestSerializingTests extends AbstractWireSerializingTestCase<GetSynonymsAction.Request> {

    @Override
    protected Writeable.Reader<GetSynonymsAction.Request> instanceReader() {
        return GetSynonymsAction.Request::new;
    }

    @Override
    protected GetSynonymsAction.Request createTestInstance() {
        if (randomBoolean()) {
            // PIT-based request
            return new GetSynonymsAction.Request(
                randomIdentifier(),
                randomIntBetween(1, 100),
                randomBoolean() ? randomIdentifier() : null,
                randomBoolean() ? randomIdentifier() : null
            );
        }
        return new GetSynonymsAction.Request(
            randomIdentifier(),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE)
        );
    }

    @Override
    protected GetSynonymsAction.Request mutateInstance(GetSynonymsAction.Request instance) throws IOException {
        String synonymsSetId = instance.synonymsSetId();
        int from = instance.from();
        int size = instance.size();
        String pitId = instance.pitId();
        String searchAfter = instance.searchAfter();

        if (pitId != null || searchAfter != null) {
            // Mutate a PIT-based request
            switch (between(0, 3)) {
                case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
                case 1 -> size = randomValueOtherThan(size, () -> randomIntBetween(1, 100));
                case 2 -> pitId = randomValueOtherThan(pitId, () -> randomBoolean() ? randomIdentifier() : null);
                case 3 -> searchAfter = randomValueOtherThan(searchAfter, () -> randomBoolean() ? randomIdentifier() : null);
                default -> throw new AssertionError("Illegal randomisation branch");
            }
            return new GetSynonymsAction.Request(synonymsSetId, size, pitId, searchAfter);
        } else {
            // Mutate a regular request
            switch (between(0, 2)) {
                case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
                case 1 -> from = randomValueOtherThan(from, () -> randomIntBetween(0, Integer.MAX_VALUE));
                case 2 -> size = randomValueOtherThan(size, () -> randomIntBetween(0, Integer.MAX_VALUE));
                default -> throw new AssertionError("Illegal randomisation branch");
            }
            return new GetSynonymsAction.Request(synonymsSetId, from, size);
        }
    }
}
