/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.apache.lucene.search.SortField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.sort.AbstractSortTestCase;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RankDocsSortBuilderTests extends AbstractSortTestCase<RankDocsSortBuilder> {

    @Override
    protected RankDocsSortBuilder createTestItem() {
        return randomRankDocsSortBuulder();
    }

    private RankDocsSortBuilder randomRankDocsSortBuulder() {
        RankDoc[] rankDocs = randomRankDocs(randomInt(100));
        return new RankDocsSortBuilder(rankDocs);
    }

    private RankDoc[] randomRankDocs(int totalDocs) {
        RankDoc[] rankDocs = new RankDoc[totalDocs];
        for (int i = 0; i < totalDocs; i++) {
            rankDocs[i] = new RankDoc(randomNonNegativeInt(), randomFloat(), randomIntBetween(0, 1));
            rankDocs[i].rank = i + 1;
        }
        return rankDocs;
    }

    @Override
    protected RankDocsSortBuilder mutate(RankDocsSortBuilder original) throws IOException {
        RankDocsSortBuilder mutated = new RankDocsSortBuilder(original);
        mutated.rankDocs(randomRankDocs(original.rankDocs().length + randomInt(100)));
        return mutated;
    }

    @Override
    public void testFromXContent() throws IOException {
        // no-op
    }

    @Override
    protected RankDocsSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        throw new UnsupportedOperationException(
            "{" + RankDocsSortBuilder.class.getSimpleName() + "} does not support parsing from XContent"
        );
    }

    @Override
    protected void sortFieldAssertions(RankDocsSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        assertThat(builder.order(), equalTo(SortOrder.ASC));
        assertThat(sortField, instanceOf(RankDocsSortField.class));
        assertThat(sortField.getField(), equalTo(RankDocsSortField.NAME));
        assertThat(sortField.getType(), equalTo(SortField.Type.CUSTOM));
        assertThat(sortField.getReverse(), equalTo(false));
    }
}
