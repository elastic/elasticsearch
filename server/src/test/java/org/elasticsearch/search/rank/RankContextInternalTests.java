/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RankContextInternalTests extends AbstractWireSerializingTestCase<RankContextInternal> {

    @Override
    protected Writeable.Reader<RankContextInternal> instanceReader() {
        return RankContextInternal::new;
    }

    @Override
    protected RankContextInternal createTestInstance() {
        List<QueryBuilder> queryBuilders = new ArrayList<>();
        int queryCount = randomIntBetween(0, 10);
        for (int qi = 0; qi < queryCount; ++qi) {
            queryBuilders.add(randomBoolean() ? createRandomTermQuery() : createRandomKnnQuery());
        }
        return new RankContextInternal(queryBuilders);
    }

    @Override
    protected RankContextInternal mutateInstance(RankContextInternal instance) throws IOException {
        List<QueryBuilder> queryBuilders = new ArrayList<>(instance.queryBuilders());
        if (queryBuilders.isEmpty()) {
            queryBuilders.add(randomBoolean() ? createRandomTermQuery() : createRandomKnnQuery());
        } else {
            queryBuilders.remove(randomInt(queryBuilders.size() - 1));
        }
        return new RankContextInternal(queryBuilders);
    }

    protected QueryBuilder createRandomTermQuery() {
        return new TermQueryBuilder(randomAlphaOfLengthBetween(1, 100), randomAlphaOfLengthBetween(1, 1000));
    }

    protected QueryBuilder createRandomKnnQuery() {
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        int numDocs = randomInt(10);
        for (int doc = 0; doc < numDocs; doc++) {
            scoreDocs.add(new ScoreDoc(doc, randomFloat()));
        }
        return new KnnScoreDocQueryBuilder(scoreDocs.toArray(new ScoreDoc[0]));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
          List.of(
              new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
              new NamedWriteableRegistry.Entry(QueryBuilder.class, KnnScoreDocQueryBuilder.NAME, KnnScoreDocQueryBuilder::new)
          )
        );
    }
}
