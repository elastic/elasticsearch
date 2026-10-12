/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.codec.columnar.DuelContext;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.codec.columnar.DuelAssertions.assertSameElements;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * The search requests and document-set comparisons the DSL behavior checks share, so a check for another field
 * type does not copy them.
 */
final class DslRequests {

    static final int MAX_HITS = 10_000;

    private DslRequests() {}

    static void assertQueryDocIds(final DuelContext ctx, final String checkName, final QueryBuilder query, final Set<Long> expected) {
        final List<Long> expectedIds = sorted(expected);
        final List<Long> baseline = sorted(docIds(ctx.client(), ctx.baselineIndex(), query, ctx.docIdField()));
        final List<Long> contender = sorted(docIds(ctx.client(), ctx.contenderIndex(), query, ctx.docIdField()));
        final String context = ctx.failureContext(checkName);
        assertSameElements(context + " stage=[baseline-oracle]", expectedIds, baseline);
        assertSameElements(context + " stage=[contender-vs-baseline]", baseline, contender);
    }

    static Set<Long> docIds(final Client client, final String index, final QueryBuilder query, final String docIdField) {
        final Set<Long> ids = new TreeSet<>();
        assertResponse(
            client.prepareSearch(index).setQuery(query).setSize(MAX_HITS).setTrackTotalHits(true).addDocValueField(docIdField),
            response -> {
                for (final SearchHit hit : response.getHits().getHits()) {
                    ids.add(((Number) hit.field(docIdField).getValue()).longValue());
                }
            }
        );
        return ids;
    }

    static List<Long> sorted(final Set<Long> ids) {
        return List.copyOf(new TreeSet<>(ids));
    }
}
