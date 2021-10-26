/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse.ReloadDetails;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ReloadDetailsTests extends AbstractWireSerializingTestCase<ReloadDetails> {

    @Override
    protected ReloadDetails createTestInstance() {
        return new ReloadDetails(randomAlphaOfLengthBetween(5, 10), Set.of(generateRandomStringArray(5, 5, false)),
                Set.of(generateRandomStringArray(5, 5, false)));
    }

    @Override
    protected Reader<ReloadDetails> instanceReader() {
        return ReloadDetails::new;
    }

    @Override
    protected ReloadDetails mutateInstance(ReloadDetails instance) throws IOException {
        String indexName = instance.getIndexName();
        Set<String> reloadedAnalyzers = new HashSet<>(instance.getReloadedAnalyzers());
        Set<String> reloadedIndicesNodes = new HashSet<>(instance.getReloadedIndicesNodes());
        int mutate = randomIntBetween(0, 2);
        switch (mutate) {
        case 0:
            indexName = indexName + randomAlphaOfLength(2);
            break;
        case 1:
            reloadedAnalyzers.add(randomAlphaOfLength(10));
            break;
        case 2:
            reloadedIndicesNodes.add(randomAlphaOfLength(10));
            break;
        default:
            throw new IllegalStateException("Requested to modify more than available parameters.");
        }
        return new ReloadDetails(indexName, reloadedIndicesNodes, reloadedAnalyzers);
    }

}
