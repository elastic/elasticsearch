/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

public class IndexSortRangeQueryNestedDocsTests extends IndexSortRangeQueryNestedDocsTestCase {

    public void testRandomBlocks() throws IOException {
        final int scenarios = between(3, 6);
        for (int scenario = 0; scenario < scenarios; scenario++) {
            randomScenario();
        }
    }

    private void randomScenario() throws IOException {
        final Kind kind = randomFrom(KINDS);
        final SortRole role = randomFrom(SortRole.values());
        final boolean points = kind.supportsPoints() && randomBoolean();
        final String order = randomFrom("asc", "desc");
        final String missing = randomFrom("_first", "_last");
        final String mode = randomFrom("min", "max");
        final List<Block> blocks = new ArrayList<>();
        final Set<Integer> flushAfter = new TreeSet<>();
        final int numBlocks = between(1, 25);
        for (int id = 0; id < numBlocks; id++) {
            final Long value = frequently() ? (long) between(0, 100) : null;
            blocks.add(new Block(id, value, between(0, 3)));
            if (rarely()) {
                flushAfter.add(id);
            }
        }
        final Long lower = randomBoolean() ? null : (long) between(0, 105);
        final Long upper = randomBoolean() ? null : (long) between(0, 105);
        final boolean includeLower = randomBoolean();
        final boolean includeUpper = randomBoolean();

        final Set<Integer> expected = new TreeSet<>();
        for (final Block block : blocks) {
            if (block.value() == null) {
                continue;
            }
            final boolean aboveLower = lower == null || (includeLower ? block.value() >= lower : block.value() > lower);
            final boolean belowUpper = upper == null || (includeUpper ? block.value() <= upper : block.value() < upper);
            if (aboveLower && belowUpper) {
                expected.add(block.id());
            }
        }

        final Result result = search(
            kind,
            role,
            points,
            order,
            missing,
            mode,
            blocks,
            flushAfter,
            lower,
            upper,
            includeLower,
            includeUpper
        );
        final String description = String.format(
            Locale.ROOT,
            "kind=%s role=%s points=%s order=%s missing=%s mode=%s blocks=%s range=%s%s,%s%s",
            kind.type(),
            role,
            points,
            order,
            missing,
            mode,
            blocks,
            includeLower ? "[" : "(",
            lower,
            upper,
            includeUpper ? "]" : ")"
        );
        assertEquals(description, new Result(expected, expected.size()), result);
    }

    public void testIndexSortRangeQueryIsSkippedWhenTheMappingHasNestedFields() throws IOException {
        for (final Kind kind : KINDS) {
            if (kind.usesIndexSortRangeQuery() == false) {
                continue;
            }
            for (final SortRole role : SortRole.values()) {
                final boolean points = randomBoolean();
                final boolean withNestedField = randomBoolean();
                final MapperService mapperService = createMapperService(
                    indexSettings(role, randomFrom("asc", "desc"), "_last", "min"),
                    mappings(kind, points, withNestedField)
                );
                final SearchExecutionContext context = createSearchExecutionContext(mapperService);
                final Query query = new RangeQueryBuilder("field").gte(20L).lte(30L).toQuery(context);
                assertEquals(role == SortRole.PRIMARY, context.indexSortedOnField("field"));
                final boolean expectWrapper = role == SortRole.PRIMARY && withNestedField == false;
                assertEquals(expectWrapper, context.canUseIndexSortRangeQuery("field"));
                assertEquals(
                    String.format(Locale.ROOT, "kind=%s role=%s nested=%s query=%s", kind.type(), role, withNestedField, query),
                    expectWrapper,
                    query instanceof IndexSortSortedNumericDocValuesRangeQuery
                );
            }
        }
    }
}
