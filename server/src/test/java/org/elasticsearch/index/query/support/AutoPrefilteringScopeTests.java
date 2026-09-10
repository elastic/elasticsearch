/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.support.AutoPrefilteringScope.ScopedPrefilter;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class AutoPrefilteringScopeTests extends ESTestCase {

    public void testMultipleLevels() {
        AutoPrefilteringScope autoPrefilteringScope = new AutoPrefilteringScope();
        assertThat(autoPrefilteringScope.getPrefilters(), is(empty()));

        List<QueryBuilder> prefilters_1_1 = randomList(0, 5, () -> RandomQueryBuilder.createQuery(random()));
        List<QueryBuilder> prefilters_1_2 = randomList(0, 5, () -> RandomQueryBuilder.createQuery(random()));
        List<QueryBuilder> prefilters_2_1 = randomList(0, 5, () -> RandomQueryBuilder.createQuery(random()));
        List<QueryBuilder> prefilters_2_2 = randomList(0, 5, () -> RandomQueryBuilder.createQuery(random()));
        List<QueryBuilder> prefilters_3_1 = randomList(0, 5, () -> RandomQueryBuilder.createQuery(random()));

        NestedObjectMapper level_1 = null;
        NestedObjectMapper level_2 = buildNestedMapper("a");
        NestedObjectMapper level_3 = buildNestedMapper("a.b");

        // Given + increases level and - decreases level, we add scope as follows:
        // + 1_1 + 2_1 + 3_1
        // - 3_1 + 2_2
        // - 2_2 + 1_2
        // - 1_2 + 1_1
        // - 1_1
        // and we check current prefilters after each operation.

        autoPrefilteringScope.push(prefilters_1_1, level_1);
        assertThat(autoPrefilteringScope.getPrefilters(), equalTo(scoped(prefilters_1_1, level_1)));
        autoPrefilteringScope.push(prefilters_2_1, level_2);
        assertThat(
            autoPrefilteringScope.getPrefilters(),
            equalTo(Stream.of(scoped(prefilters_2_1, level_2), scoped(prefilters_1_1, level_1)).flatMap(Collection::stream).toList())
        );
        autoPrefilteringScope.push(prefilters_3_1, level_3);
        assertThat(
            autoPrefilteringScope.getPrefilters(),
            equalTo(
                Stream.of(scoped(prefilters_3_1, level_3), scoped(prefilters_2_1, level_2), scoped(prefilters_1_1, level_1))
                    .flatMap(Collection::stream)
                    .toList()
            )
        );
        autoPrefilteringScope.pop();
        assertThat(
            autoPrefilteringScope.getPrefilters(),
            equalTo(Stream.of(scoped(prefilters_2_1, level_2), scoped(prefilters_1_1, level_1)).flatMap(Collection::stream).toList())
        );
        autoPrefilteringScope.push(prefilters_2_2, level_2);
        assertThat(
            autoPrefilteringScope.getPrefilters(),
            equalTo(
                Stream.of(scoped(prefilters_2_2, level_2), scoped(prefilters_2_1, level_2), scoped(prefilters_1_1, level_1))
                    .flatMap(Collection::stream)
                    .toList()
            )
        );
        autoPrefilteringScope.pop();
        assertThat(
            autoPrefilteringScope.getPrefilters(),
            equalTo(Stream.of(scoped(prefilters_2_1, level_2), scoped(prefilters_1_1, level_1)).flatMap(Collection::stream).toList())
        );
        autoPrefilteringScope.pop();
        assertThat(autoPrefilteringScope.getPrefilters(), equalTo(scoped(prefilters_1_1, level_1)));
        autoPrefilteringScope.push(prefilters_1_2, level_1);
        assertThat(
            autoPrefilteringScope.getPrefilters(),
            equalTo(Stream.of(scoped(prefilters_1_2, level_1), scoped(prefilters_1_1, level_1)).flatMap(Collection::stream).toList())
        );
        autoPrefilteringScope.pop();
        assertThat(autoPrefilteringScope.getPrefilters(), equalTo(scoped(prefilters_1_1, level_1)));
        autoPrefilteringScope.pop();
        assertThat(autoPrefilteringScope.getPrefilters(), empty());
    }

    public void testReleasable() {
        AutoPrefilteringScope autoPrefilteringScope = new AutoPrefilteringScope();
        List<QueryBuilder> prefilters_1 = randomList(0, 5, () -> RandomQueryBuilder.createQuery(random()));
        List<QueryBuilder> prefilters_2 = randomList(0, 5, () -> RandomQueryBuilder.createQuery(random()));

        try (autoPrefilteringScope) {
            autoPrefilteringScope.push(prefilters_1, null);

            try (autoPrefilteringScope) {
                autoPrefilteringScope.push(prefilters_2, null);
                assertThat(
                    autoPrefilteringScope.getPrefilters(),
                    equalTo(Stream.of(scoped(prefilters_2, null), scoped(prefilters_1, null)).flatMap(Collection::stream).toList())
                );
            }
            assertThat(autoPrefilteringScope.getPrefilters(), equalTo(scoped(prefilters_1, null)));
        }
        assertThat(autoPrefilteringScope.getPrefilters(), is(empty()));
    }

    private static List<ScopedPrefilter> scoped(List<QueryBuilder> prefilters, NestedObjectMapper nestedLevel) {
        return prefilters.stream().map(q -> new ScopedPrefilter(q, nestedLevel)).toList();
    }

    private static NestedObjectMapper buildNestedMapper(String path) {
        return new NestedObjectMapper.Builder(path, IndexVersion.current(), query -> { throw new UnsupportedOperationException(); }, null)
            .build(MapperBuilderContext.root(false, false));
    }
}
