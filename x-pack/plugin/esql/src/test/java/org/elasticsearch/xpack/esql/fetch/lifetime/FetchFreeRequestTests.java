/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;
import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class FetchFreeRequestTests extends AbstractWireSerializingTestCase<FetchFreeRequest> {
    @Override
    protected Writeable.Reader<FetchFreeRequest> instanceReader() {
        return FetchFreeRequest::new;
    }

    @Override
    protected FetchFreeRequest createTestInstance() {
        return new FetchFreeRequest(new OriginalIndices(randomIndices(), randomIndicesOptions()), randomContextIds());
    }

    @Override
    protected FetchFreeRequest mutateInstance(FetchFreeRequest instance) {
        String[] indices = instance.indices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        List<ShardSearchContextId> contextIds = instance.contextIds();
        switch (between(0, 2)) {
            case 0 -> indices = randomArrayOtherThan(indices, FetchFreeRequestTests::randomIndices);
            case 1 -> indicesOptions = randomValueOtherThan(indicesOptions, FetchFreeRequestTests::randomIndicesOptions);
            case 2 -> contextIds = randomValueOtherThan(contextIds, FetchFreeRequestTests::randomContextIds);
            default -> throw new AssertionError("unknown field");
        }
        return new FetchFreeRequest(new OriginalIndices(indices, indicesOptions), contextIds);
    }

    /**
     * The request is authorized like the query: the resolver expands its expressions to the indices the user can read.
     */
    public void testIsAuthorizedWithTheIndexExpressionsOfTheQuery() {
        FetchFreeRequest request = new FetchFreeRequest(
            new OriginalIndices(new String[] { "logs-*" }, IndicesOptions.strictExpandOpen()),
            randomContextIds()
        );
        assertTrue(request.includeDataStreams());
        assertThat(request.indices(), equalTo(new String[] { "logs-*" }));

        request.indices("logs-1", "logs-2");
        assertThat(request.indices(), equalTo(new String[] { "logs-1", "logs-2" }));
    }

    /**
     * A user who can read none of the indices anymore frees nothing. The reaper frees the contexts later.
     */
    public void testFreesNothingWithoutAnAuthorizedIndex() {
        FetchFreeRequest request = createTestInstance();
        if (randomBoolean()) {
            request.indices(NO_INDICES_OR_ALIASES_ARRAY);
        } else {
            request.indices(NO_INDEX_PLACEHOLDER);
        }
        assertThat(request.contextIds(), empty());
    }

    private static String[] randomIndices() {
        return generateRandomStringArray(5, 10, false, false);
    }

    private static IndicesOptions randomIndicesOptions() {
        return IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
    }

    private static List<ShardSearchContextId> randomContextIds() {
        return randomList(
            1,
            5,
            () -> new ShardSearchContextId(
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomBoolean() ? null : randomAlphaOfLength(8)
            )
        );
    }
}
