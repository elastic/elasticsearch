/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class GetSynonymsActionRequestSerializingTests extends AbstractWireSerializingTestCase<GetSynonymsAction.Request> {

    private final boolean cursorBased;

    public GetSynonymsActionRequestSerializingTests(boolean cursorBased) {
        this.cursorBased = cursorBased;
    }

    @ParametersFactory(argumentFormatting = "cursorBased=%s")
    public static List<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Override
    protected Writeable.Reader<GetSynonymsAction.Request> instanceReader() {
        return GetSynonymsAction.Request::new;
    }

    @Override
    protected GetSynonymsAction.Request createTestInstance() {
        String synonymsSetId = randomIdentifier();
        int size = randomIntBetween(0, Integer.MAX_VALUE);
        if (cursorBased) {
            return new GetSynonymsAction.Request(synonymsSetId, randomBoolean() ? null : randomIdentifier(), size);
        } else {
            return new GetSynonymsAction.Request(synonymsSetId, randomIntBetween(0, Integer.MAX_VALUE), size);
        }
    }

    @Override
    protected GetSynonymsAction.Request mutateInstance(GetSynonymsAction.Request instance) throws IOException {
        String synonymsSetId = instance.synonymsSetId();
        int size = instance.size();
        if (cursorBased) {
            String searchAfter = instance.searchAfter();
            return switch (between(0, 2)) {
                case 0 -> new GetSynonymsAction.Request(randomValueOtherThan(synonymsSetId, () -> randomIdentifier()), searchAfter, size);
                case 1 -> new GetSynonymsAction.Request(
                    synonymsSetId,
                    randomValueOtherThan(searchAfter, () -> randomBoolean() ? null : randomIdentifier()),
                    size
                );
                case 2 -> new GetSynonymsAction.Request(
                    synonymsSetId,
                    searchAfter,
                    randomValueOtherThan(size, () -> randomIntBetween(0, Integer.MAX_VALUE))
                );
                default -> throw new AssertionError("Illegal randomisation branch");
            };
        } else {
            int from = instance.from();
            return switch (between(0, 2)) {
                case 0 -> new GetSynonymsAction.Request(randomValueOtherThan(synonymsSetId, () -> randomIdentifier()), from, size);
                case 1 -> new GetSynonymsAction.Request(
                    synonymsSetId,
                    randomValueOtherThan(from, () -> randomIntBetween(0, Integer.MAX_VALUE)),
                    size
                );
                case 2 -> new GetSynonymsAction.Request(
                    synonymsSetId,
                    from,
                    randomValueOtherThan(size, () -> randomIntBetween(0, Integer.MAX_VALUE))
                );
                default -> throw new AssertionError("Illegal randomisation branch");
            };
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
