/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class KnnEvalRequestTests extends AbstractWireSerializingTestCase<KnnEvalRequest> {

    /** The spec's optional {@code filter} is a named writeable {@link org.elasticsearch.index.query.QueryBuilder}. */
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return KnnEvalSpecTests.NAMED_WRITEABLE_REGISTRY;
    }

    @Override
    protected KnnEvalRequest createTestInstance() {
        String[] indices = new String[randomIntBetween(1, 3)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        KnnEvalRequest request = new KnnEvalRequest(KnnEvalSpecTests.createTestItem(), indices);
        request.indicesOptions(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            )
        );
        return request;
    }

    @Override
    protected Reader<KnnEvalRequest> instanceReader() {
        return KnnEvalRequest::new;
    }

    @Override
    protected KnnEvalRequest mutateInstance(KnnEvalRequest instance) throws IOException {
        KnnEvalRequest mutation = copyInstance(instance);
        switch (randomIntBetween(0, 2)) {
            case 0 -> mutation.indices(ArrayUtils.concat(instance.indices(), new String[] { randomAlphaOfLength(10) }));
            case 1 -> mutation.indicesOptions(
                randomValueOtherThan(
                    instance.indicesOptions(),
                    () -> IndicesOptions.fromOptions(
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean()
                    )
                )
            );
            case 2 -> mutation.setKnnEvalSpec(KnnEvalSpecTests.mutateTestItem(instance.getKnnEvalSpec()));
            default -> throw new AssertionError("unreachable");
        }
        return mutation;
    }

    public void testAtLeastOneIndexIsRequired() {
        KnnEvalRequest request = new KnnEvalRequest(KnnEvalSpecTests.createTestItem(), new String[0]);
        assertNotNull(request.validate());
        assertEquals(1, request.validate().validationErrors().size());
        assertEquals("at least one index must be specified", request.validate().validationErrors().get(0));
    }

    public void testMissingSpecIsRejected() {
        KnnEvalRequest request = new KnnEvalRequest();
        assertNotNull(request.validate());
        assertTrue(request.validate().validationErrors().contains("missing knn evaluation specification"));
    }
}
