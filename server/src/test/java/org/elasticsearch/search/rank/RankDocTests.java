/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RankDocTests extends AbstractRankDocWireSerializingTestCase<RankDoc> {

    protected RankDoc createTestRankDoc() {
        RankDoc rankDoc = new RankDoc(randomNonNegativeInt(), randomFloat(), randomIntBetween(0, 1));
        rankDoc.rank = randomNonNegativeInt();
        return rankDoc;
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> getAdditionalNamedWriteables() {
        return Collections.emptyList();
    }

    @Override
    protected Writeable.Reader<RankDoc> instanceReader() {
        return RankDoc::new;
    }

    @Override
    protected RankDoc mutateInstance(RankDoc instance) throws IOException {
        RankDoc mutated = new RankDoc(instance.doc, instance.score, instance.shardIndex);
        mutated.rank = instance.rank;
        if (frequently()) {
            mutated.doc = randomNonNegativeInt();
        }
        if (frequently()) {
            mutated.score = randomFloat();
        }
        if (frequently()) {
            mutated.shardIndex = randomNonNegativeInt();
        }
        if (frequently()) {
            mutated.rank = randomNonNegativeInt();
        }
        return mutated;
    }
}
