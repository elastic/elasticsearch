/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse.ReloadDetails;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashSet;
import java.util.Set;

public class ReloadDetailsTests extends AbstractWireSerializingTestCase<ReloadDetails> {

    @Override
    protected ReloadDetails createTestInstance() {
        return new ReloadDetails(
            randomAlphaOfLengthBetween(5, 10),
            Set.of(generateRandomStringArray(5, 5, false)),
            Set.of(generateRandomStringArray(5, 5, false))
        );
    }

    @Override
    protected Reader<ReloadDetails> instanceReader() {
        return ReloadDetails::new;
    }

    @Override
    protected ReloadDetails mutateInstance(ReloadDetails instance) {
        String indexName = instance.getIndexName();
        Set<String> reloadedAnalyzers = new HashSet<>(instance.getReloadedAnalyzers());
        Set<String> reloadedIndicesNodes = new HashSet<>(instance.getReloadedIndicesNodes());
        int mutate = randomIntBetween(0, 2);
        switch (mutate) {
            case 0 -> indexName = indexName + randomAlphaOfLength(2);
            case 1 -> reloadedAnalyzers.add(randomAlphaOfLength(10));
            case 2 -> reloadedIndicesNodes.add(randomAlphaOfLength(10));
            default -> throw new IllegalStateException("Requested to modify more than available parameters.");
        }
        return new ReloadDetails(indexName, reloadedIndicesNodes, reloadedAnalyzers);
    }

}
