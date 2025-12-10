/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.InterceptedQueryBuilderWrapper;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.index.query.support.AutoPrefilteringUtils.pruneQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AutoPrefilteringUtilsTests extends ESTestCase {

    public void testPruneQuery_GivenSingleQuery_NonPrunedType() {
        QueryBuilder query = RandomQueryBuilder.createQuery(random());

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(NestedQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query));
    }

    public void testPruneQuery_GivenSingleQuery_PrunedType() {
        QueryBuilder query = RandomQueryBuilder.createQuery(random());

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(query.getClass()));

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenBoolQuery_NonPrunedType() {
        BoolQueryBuilder query = boolQuery(
            new BoolClause(RandomQueryBuilder.createQuery(random()), randomFrom(BooleanClause.Occur.values()))
        );

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(NestedQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query));
    }

    public void testPruneQuery_GivenBoolQuery_PrunedType() {
        QueryBuilder clause = RandomQueryBuilder.createQuery(random());
        BoolQueryBuilder query = boolQuery(new BoolClause(clause, randomFrom(BooleanClause.Occur.values())));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(clause.getClass()));

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenBoolQuery_WithShouldClauses_AndMinShouldMatchNumericPositive() {
        List<QueryBuilder> prunedClauses = randomList(1, 5, () -> new TermQueryBuilder(randomAlphaOfLength(5), randomInt()));
        List<QueryBuilder> nonPrunedClauses = randomList(1, 5, () -> new RangeQueryBuilder(randomAlphaOfLength(5)));
        BoolQueryBuilder query = new BoolQueryBuilder();
        prunedClauses.forEach(query::should);
        nonPrunedClauses.forEach(query::should);
        int originalMsm = randomIntBetween(1, prunedClauses.size() + nonPrunedClauses.size());
        query.minimumShouldMatch(originalMsm);

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(TermQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        BoolQueryBuilder expected = new BoolQueryBuilder();
        nonPrunedClauses.forEach(expected::should);
        expected.minimumShouldMatch(originalMsm <= nonPrunedClauses.size() ? originalMsm : nonPrunedClauses.size());
        assertThat(pruned.get(), equalTo(expected));
    }

    public void testPruneQuery_GivenBoolQuery_WithShouldClauses_AndMinShouldMatchNumericNegative() {
        List<QueryBuilder> prunedClauses = randomList(1, 5, () -> new TermQueryBuilder(randomAlphaOfLength(5), randomInt()));
        List<QueryBuilder> nonPrunedClauses = randomList(1, 5, () -> new RangeQueryBuilder(randomAlphaOfLength(5)));
        BoolQueryBuilder query = new BoolQueryBuilder();
        prunedClauses.forEach(query::should);
        nonPrunedClauses.forEach(query::should);
        int originalMsm = -randomIntBetween(1, prunedClauses.size() + nonPrunedClauses.size());
        query.minimumShouldMatch(originalMsm);

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(TermQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        BoolQueryBuilder expected = new BoolQueryBuilder();
        nonPrunedClauses.forEach(expected::should);
        // We expect num_pruned + num_non_pruned + originalMsm (adding as it's negative) - num_pruned = non_pruned + originalMsm
        int originalPositiveMsm = query.should().size() + originalMsm;
        expected.minimumShouldMatch(originalPositiveMsm <= nonPrunedClauses.size() ? originalPositiveMsm : nonPrunedClauses.size());
        assertThat(pruned.get(), equalTo(expected));
    }

    public void testPruneQuery_GivenBoolQuery_WithShouldClauses_AndMinShouldMatchPercentage() {
        List<QueryBuilder> prunedClauses = randomList(2, 2, () -> new TermQueryBuilder(randomAlphaOfLength(5), randomInt()));
        List<QueryBuilder> nonPrunedClauses = randomList(2, 2, () -> new RangeQueryBuilder(randomAlphaOfLength(5)));
        BoolQueryBuilder query = new BoolQueryBuilder();
        prunedClauses.forEach(query::should);
        nonPrunedClauses.forEach(query::should);
        query.minimumShouldMatch("75%");

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(TermQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        BoolQueryBuilder expected = new BoolQueryBuilder();
        nonPrunedClauses.forEach(expected::should);
        expected.minimumShouldMatch(2);
        assertThat(pruned.get(), equalTo(expected));
    }

    public void testPruneQuery_GivenBoolQuery_WithBoolQueryWithTwoClauses_OneShouldBePruned() {
        QueryBuilder prunedClause = RandomQueryBuilder.createQuery(random());
        BooleanClause.Occur prunedOccur = randomFrom(BooleanClause.Occur.values());
        QueryBuilder nonPrunedClause = randomQueryOfDifferentType(prunedClause.getClass());
        BooleanClause.Occur nonPrunedOccur = randomFrom(BooleanClause.Occur.values());
        BoolQueryBuilder boolQuery = boolQuery(new BoolClause(prunedClause, prunedOccur), new BoolClause(nonPrunedClause, nonPrunedOccur));

        BoolQueryBuilder query = new BoolQueryBuilder().must(boolQuery).adjustPureNegative(randomBoolean()).minimumShouldMatch(randomInt());

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(prunedClause.getClass()));

        assertThat(pruned.isPresent(), is(true));
        assertThat(
            pruned.get(),
            equalTo(
                new BoolQueryBuilder().must(boolQuery(new BoolClause(nonPrunedClause, nonPrunedOccur)))
                    .minimumShouldMatch(query.minimumShouldMatch())
            )
        );
    }

    public void testPruneQuery_GivenBoostingQuery_NonPrunedType() {
        BoostingQueryBuilder query = new BoostingQueryBuilder(
            RandomQueryBuilder.createQuery(random()),
            RandomQueryBuilder.createQuery(random())
        );

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(NestedQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query.positiveQuery()));
    }

    public void testPruneQuery_GivenBoostingQuery_PrunedType() {
        BoostingQueryBuilder query = new BoostingQueryBuilder(
            RandomQueryBuilder.createQuery(random()),
            RandomQueryBuilder.createQuery(random())
        );

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(query.positiveQuery().getClass()));

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenBoostingQuery_WithBoolQueryWithTwoClauses_OneShouldBePruned() {
        QueryBuilder prunedClause = RandomQueryBuilder.createQuery(random());
        BooleanClause.Occur prunedOccur = randomFrom(BooleanClause.Occur.values());
        QueryBuilder nonPrunedClause = randomQueryOfDifferentType(prunedClause.getClass());
        BooleanClause.Occur nonPrunedOccur = randomFrom(BooleanClause.Occur.values());

        BoolQueryBuilder boolQuery = boolQuery(new BoolClause(prunedClause, prunedOccur), new BoolClause(nonPrunedClause, nonPrunedOccur));
        BoostingQueryBuilder query = new BoostingQueryBuilder(boolQuery, RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(prunedClause.getClass()));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), equalTo(boolQuery(new BoolClause(nonPrunedClause, nonPrunedOccur))));
    }

    public void testPruneQuery_GivenConstantScoreQuery_NonPrunedType() {
        QueryBuilder query = new ConstantScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(NestedQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query));
    }

    public void testPruneQuery_GivenConstantScoreQuery_PrunedType() {
        ConstantScoreQueryBuilder query = new ConstantScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(query.innerQuery().getClass()));

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenConstantScoreQuery_WithBoolQueryWithTwoClauses_OneShouldBePruned() {
        QueryBuilder prunedClause = RandomQueryBuilder.createQuery(random());
        BooleanClause.Occur prunedOccur = randomFrom(BooleanClause.Occur.values());
        QueryBuilder nonPrunedClause = randomQueryOfDifferentType(prunedClause.getClass());
        BooleanClause.Occur nonPrunedOccur = randomFrom(BooleanClause.Occur.values());

        BoolQueryBuilder boolQuery = boolQuery(new BoolClause(prunedClause, prunedOccur), new BoolClause(nonPrunedClause, nonPrunedOccur));
        ConstantScoreQueryBuilder query = new ConstantScoreQueryBuilder(boolQuery);

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(prunedClause.getClass()));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), equalTo(new ConstantScoreQueryBuilder(boolQuery(new BoolClause(nonPrunedClause, nonPrunedOccur)))));
    }

    public void testPruneQuery_GivenDisMaxQuery_NonPrunedType() {
        QueryBuilder query = new DisMaxQueryBuilder().add(RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(NestedQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query));
    }

    public void testPruneQuery_GivenDisMaxQuery_PrunedType() {
        DisMaxQueryBuilder query = new DisMaxQueryBuilder().add(RandomQueryBuilder.createQuery(random()))
            .add(RandomQueryBuilder.createQuery(random()));
        boolean addSecondClauseType = query.innerQueries().get(0).getClass() != query.innerQueries().get(1).getClass();

        Optional<QueryBuilder> pruned = pruneQuery(
            query,
            addSecondClauseType
                ? Set.of(query.innerQueries().get(0).getClass(), query.innerQueries().get(1).getClass())
                : Set.of(query.innerQueries().get(0).getClass())
        );

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenDisMaxQuery_WithBoolQueryWithTwoClauses_OneShouldBePruned() {
        QueryBuilder prunedClause = RandomQueryBuilder.createQuery(random());
        BooleanClause.Occur prunedOccur = randomFrom(BooleanClause.Occur.values());
        QueryBuilder nonPrunedClause = randomQueryOfDifferentType(prunedClause.getClass());
        BooleanClause.Occur nonPrunedOccur = randomFrom(BooleanClause.Occur.values());

        BoolQueryBuilder boolQuery = boolQuery(new BoolClause(prunedClause, prunedOccur), new BoolClause(nonPrunedClause, nonPrunedOccur));
        DisMaxQueryBuilder query = new DisMaxQueryBuilder().add(boolQuery).tieBreaker(randomFloat());

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(prunedClause.getClass()));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), equalTo(new DisMaxQueryBuilder().add(boolQuery(new BoolClause(nonPrunedClause, nonPrunedOccur)))));
    }

    public void testPruneQuery_GivenFunctionScoreQuery_NonPrunedType() {
        QueryBuilder query = new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(NestedQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query));
    }

    public void testPruneQuery_GivenFunctionScoreQuery_PrunedType() {
        FunctionScoreQueryBuilder query = new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(query.query().getClass()));

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenFunctionScoreQuery_WithBoolQueryWithTwoClauses_OneShouldBePruned() {
        QueryBuilder prunedClause = RandomQueryBuilder.createQuery(random());
        BooleanClause.Occur prunedOccur = randomFrom(BooleanClause.Occur.values());
        QueryBuilder nonPrunedClause = randomQueryOfDifferentType(prunedClause.getClass());
        BooleanClause.Occur nonPrunedOccur = randomFrom(BooleanClause.Occur.values());

        BoolQueryBuilder boolQuery = boolQuery(new BoolClause(prunedClause, prunedOccur), new BoolClause(nonPrunedClause, nonPrunedOccur));
        FunctionScoreQueryBuilder query = new FunctionScoreQueryBuilder(boolQuery);

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(prunedClause.getClass()));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), equalTo(new FunctionScoreQueryBuilder(boolQuery(new BoolClause(nonPrunedClause, nonPrunedOccur)))));
    }

    public void testPruneQuery_GivenInterceptedQuery_NonPrunedType() {
        QueryBuilder query = new InterceptedQueryBuilderWrapper(RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(NestedQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query));
    }

    public void testPruneQuery_GivenInterceptedQuery_PrunedType() {
        InterceptedQueryBuilderWrapper query = new InterceptedQueryBuilderWrapper(RandomQueryBuilder.createQuery(random()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(query.query().getClass()));

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenInterceptedQuery_WithBoolQueryWithTwoClauses_OneShouldBePruned() {
        QueryBuilder prunedClause = RandomQueryBuilder.createQuery(random());
        BooleanClause.Occur prunedOccur = randomFrom(BooleanClause.Occur.values());
        QueryBuilder nonPrunedClause = randomQueryOfDifferentType(prunedClause.getClass());
        BooleanClause.Occur nonPrunedOccur = randomFrom(BooleanClause.Occur.values());

        BoolQueryBuilder boolQuery = boolQuery(new BoolClause(prunedClause, prunedOccur), new BoolClause(nonPrunedClause, nonPrunedOccur));
        InterceptedQueryBuilderWrapper query = new InterceptedQueryBuilderWrapper(boolQuery);

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(prunedClause.getClass()));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), equalTo(new InterceptedQueryBuilderWrapper(boolQuery(new BoolClause(nonPrunedClause, nonPrunedOccur)))));
    }

    public void testPruneQuery_GivenNestedQuery_NonPrunedType() {
        QueryBuilder query = new NestedQueryBuilder(
            randomAlphaOfLength(10),
            RandomQueryBuilder.createQuery(random()),
            randomFrom(ScoreMode.values())
        );

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(BoolQueryBuilder.class));

        assertThat(pruned.isPresent(), is(true));
        assertThat(pruned.get(), sameInstance(query));
    }

    public void testPruneQuery_GivenNestedQuery_PrunedType() {
        NestedQueryBuilder query = new NestedQueryBuilder(
            randomAlphaOfLength(10),
            RandomQueryBuilder.createQuery(random()),
            randomFrom(ScoreMode.values())
        );

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(query.query().getClass()));

        assertThat(pruned.isEmpty(), is(true));
    }

    public void testPruneQuery_GivenNestedQuery_WithBoolQueryWithTwoClauses_OneShouldBePruned() {
        QueryBuilder prunedClause = RandomQueryBuilder.createQuery(random());
        BooleanClause.Occur prunedOccur = randomFrom(BooleanClause.Occur.values());
        QueryBuilder nonPrunedClause = randomQueryOfDifferentType(prunedClause.getClass());
        BooleanClause.Occur nonPrunedOccur = randomFrom(BooleanClause.Occur.values());

        BoolQueryBuilder boolQuery = boolQuery(new BoolClause(prunedClause, prunedOccur), new BoolClause(nonPrunedClause, nonPrunedOccur));
        NestedQueryBuilder query = new NestedQueryBuilder(randomAlphaOfLength(10), boolQuery, randomFrom(ScoreMode.values()));

        Optional<QueryBuilder> pruned = pruneQuery(query, Set.of(prunedClause.getClass()));

        assertThat(pruned.isPresent(), is(true));
        assertThat(
            pruned.get(),
            equalTo(new NestedQueryBuilder(query.path(), boolQuery(new BoolClause(nonPrunedClause, nonPrunedOccur)), query.scoreMode()))
        );
    }

    private static QueryBuilder randomQueryOfDifferentType(Class<? extends QueryBuilder> type) {
        QueryBuilder query = RandomQueryBuilder.createQuery(random());
        if (query.getClass() == type) {
            return randomQueryOfDifferentType(type);
        } else {
            return query;
        }
    }

    private static BoolQueryBuilder boolQuery(BoolClause... clauses) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (BoolClause clause : clauses) {
            switch (clause.occur()) {
                case MUST -> boolQuery.must(clause.query());
                case SHOULD -> boolQuery.should(clause.query());
                case FILTER -> boolQuery.filter(clause.query());
                case MUST_NOT -> boolQuery.mustNot(clause.query());
            }
        }
        return boolQuery;
    }

    private record BoolClause(QueryBuilder query, BooleanClause.Occur occur) {};
}
