/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Constants;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.Case;
import org.elasticsearch.index.codec.vectors.VectorReadHintsTests.Open;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;
import java.util.Optional;

/** The advice the directory derives for search from what each vectors file says: only raw vectors kept to rescore. */
public class VectorReadAdviceTests extends MapperServiceTestCase {

    private static final Optional<ReadAdvice> UNADVISED = Optional.of(Constants.DEFAULT_READADVICE);

    public void testOnlyRawVectorsKeptToRescoreAreAdvisedForSearch() throws IOException {
        var advice = FsDirectoryFactory.getReadAdviceFunc();
        for (Case each : VectorReadHintsTests.cases(this)) {
            for (Open open : VectorReadHintsTests.searchOpens(each.codec())) {
                if (open.isVectorData() == false) {
                    continue;
                }
                Optional<ReadAdvice> expected = open.isRawVectors() && each.rescoresFromRaw() ? Optional.of(ReadAdvice.RANDOM) : UNADVISED;
                assertEquals(each + ": " + open, expected, advice.apply(open.name(), open.context()));
            }
        }
    }
}
