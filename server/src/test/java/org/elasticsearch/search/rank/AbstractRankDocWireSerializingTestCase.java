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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.retriever.rankdoc.RankDocsQueryBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractRankDocWireSerializingTestCase<T extends RankDoc> extends AbstractWireSerializingTestCase<T> {

    protected abstract T createTestRankDoc();

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = searchModule.getNamedWriteables();
        entries.addAll(getAdditionalNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    protected abstract List<NamedWriteableRegistry.Entry> getAdditionalNamedWriteables();

    @Override
    protected T createTestInstance() {
        return createTestRankDoc();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testRankDocSerialization() throws IOException {
        int totalDocs = randomIntBetween(10, 100);
        Set<T> docs = new HashSet<>();
        for (int i = 0; i < totalDocs; i++) {
            docs.add(createTestRankDoc());
        }
        RankDocsQueryBuilder rankDocsQueryBuilder = new RankDocsQueryBuilder(docs.toArray((T[]) new RankDoc[0]), null, randomBoolean());
        RankDocsQueryBuilder copy = (RankDocsQueryBuilder) copyNamedWriteable(rankDocsQueryBuilder, writableRegistry(), QueryBuilder.class);
        assertThat(rankDocsQueryBuilder, equalTo(copy));
    }
}
