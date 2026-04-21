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

import static org.hamcrest.Matchers.containsString;

public class GetSynonymsActionRequestSerializingTests extends AbstractWireSerializingTestCase<GetSynonymsAction.Request> {

    @Override
    protected Writeable.Reader<GetSynonymsAction.Request> instanceReader() {
        return GetSynonymsAction.Request::new;
    }

    @Override
    protected GetSynonymsAction.Request createTestInstance() {
        String synonymsSetId = randomIdentifier();
        int size = randomIntBetween(0, Integer.MAX_VALUE);
        if (randomBoolean()) {
            // legacy offset-based: from is always > 0 so the two paths are distinguishable
            return new GetSynonymsAction.Request(synonymsSetId, randomIntBetween(1, Integer.MAX_VALUE), size);
        } else {
            // cursor-based: from is always 0, searchAfter may be null (first page) or a rule ID
            return new GetSynonymsAction.Request(synonymsSetId, randomBoolean() ? null : randomIdentifier(), size);
        }
    }

    @Override
    protected GetSynonymsAction.Request mutateInstance(GetSynonymsAction.Request instance) throws IOException {
        String synonymsSetId = instance.synonymsSetId();
        int from = instance.from();
        int size = instance.size();
        String searchAfter = instance.searchAfter();
        if (from > 0) {
            // legacy instance — mutate one of the three legacy fields
            switch (between(0, 2)) {
                case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
                case 1 -> from = randomValueOtherThan(from, () -> randomIntBetween(1, Integer.MAX_VALUE));
                case 2 -> size = randomValueOtherThan(size, () -> randomIntBetween(0, Integer.MAX_VALUE));
                default -> throw new AssertionError("Illegal randomisation branch");
            }
            return new GetSynonymsAction.Request(synonymsSetId, from, size);
        } else {
            // cursor instance — mutate one of the three cursor fields
            switch (between(0, 2)) {
                case 0 -> synonymsSetId = randomValueOtherThan(synonymsSetId, () -> randomIdentifier());
                case 1 -> size = randomValueOtherThan(size, () -> randomIntBetween(0, Integer.MAX_VALUE));
                case 2 -> searchAfter = randomValueOtherThan(searchAfter, () -> randomBoolean() ? null : randomIdentifier());
                default -> throw new AssertionError("Illegal randomisation branch");
            }
            return new GetSynonymsAction.Request(synonymsSetId, searchAfter, size);
        }
    }

    public void testValidationRejectsOversizedPage() {
        int overLimit = randomIntBetween(10_001, Integer.MAX_VALUE);

        // cursor-based request with size over the limit
        var cursorRequest = new GetSynonymsAction.Request("my-set", (String) null, overLimit);
        var cursorValidation = cursorRequest.validate();
        assertNotNull("expected validation error for size=" + overLimit, cursorValidation);
        assertThat(cursorValidation.getMessage(), containsString("[size] must be less than or equal to 10000"));

        // legacy offset-based request with size over the limit
        var legacyRequest = new GetSynonymsAction.Request("my-set", 0, overLimit);
        var legacyValidation = legacyRequest.validate();
        assertNotNull("expected validation error for size=" + overLimit, legacyValidation);
        assertThat(legacyValidation.getMessage(), containsString("[size] must be less than or equal to 10000"));
    }

    public void testValidationRejectsNegativeFrom() {
        var request = new GetSynonymsAction.Request("my-set", randomIntBetween(-1000, -1), randomIntBetween(0, 10));
        var validation = request.validate();
        assertNotNull(validation);
        assertThat(validation.getMessage(), containsString("[from] must be a positive integer"));
    }
}
