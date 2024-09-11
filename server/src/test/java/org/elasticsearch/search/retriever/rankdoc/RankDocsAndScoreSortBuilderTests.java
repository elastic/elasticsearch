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

public class RankDocsAndScoreSortBuilderTests extends AbstractSortTestCase<RankDocsAndScoreSortBuilder> {

    @Override
    protected RankDocsAndScoreSortBuilder createTestItem() {
        return randomRankDocsSortBuulder();
    }

    private RankDocsAndScoreSortBuilder randomRankDocsSortBuulder() {
        RankDoc[] rankDocs = randomRankDocs(randomInt(100));
        return new RankDocsAndScoreSortBuilder(rankDocs);
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
    protected RankDocsAndScoreSortBuilder mutate(RankDocsAndScoreSortBuilder original) throws IOException {
        RankDocsAndScoreSortBuilder mutated = new RankDocsAndScoreSortBuilder(original);
        mutated.rankDocs(randomRankDocs(original.rankDocs().length + randomIntBetween(10, 100)));
        return mutated;
    }

    @Override
    public void testFromXContent() throws IOException {
        // no-op
    }

    @Override
    protected RankDocsAndScoreSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        throw new UnsupportedOperationException(
            "{" + RankDocsAndScoreSortBuilder.class.getSimpleName() + "} does not support parsing from XContent"
        );
    }

    @Override
    protected void sortFieldAssertions(RankDocsAndScoreSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        assertThat(builder.order(), equalTo(SortOrder.ASC));
        assertThat(sortField, instanceOf(RankDocsAndScoreSortField.class));
        assertThat(sortField.getField(), equalTo(RankDocsAndScoreSortField.NAME));
        assertThat(sortField.getType(), equalTo(SortField.Type.CUSTOM));
        assertThat(sortField.getReverse(), equalTo(false));
    }
}
