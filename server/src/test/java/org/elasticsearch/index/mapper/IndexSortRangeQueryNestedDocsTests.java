/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;

public class IndexSortRangeQueryNestedDocsTests extends MapperServiceTestCase {

    private record Kind(String type, LongFunction<Object> value, boolean supportsPoints, boolean usesIndexSortRangeQuery) {}

    private record Block(int id, Long value, int children) {}

    private record Result(Set<Integer> ids, int count) {}

    private enum SortRole {
        PRIMARY,
        SECONDARY,
        NOT_A_SORT_FIELD,
        UNSORTED
    }

    private static final List<Kind> KINDS = List.of(
        new Kind("date", value -> value, true, true),
        new Kind("date_nanos", value -> value, true, true),
        new Kind("long", value -> value, true, true),
        new Kind("integer", value -> value, true, true),
        new Kind("short", value -> value, true, true),
        new Kind("byte", value -> value, true, true),
        new Kind("keyword", value -> String.format(Locale.ROOT, "v%03d", value), false, false)
    );

    public void testChildrenBeforeTheMatchingParent() throws IOException {
        final List<Block> blocks = List.of(new Block(0, 10L, 1), new Block(1, 20L, 1), new Block(2, 30L, 1));
        final List<String> failures = new ArrayList<>();
        for (final Kind kind : KINDS) {
            for (final String order : List.of("asc", "desc")) {
                for (final boolean points : List.of(false, true)) {
                    if (points && kind.supportsPoints() == false) {
                        continue;
                    }
                    final Result result = search(
                        kind,
                        SortRole.PRIMARY,
                        points,
                        order,
                        "_last",
                        "min",
                        blocks,
                        Set.of(),
                        20L,
                        20L,
                        true,
                        true
                    );
                    if (result.equals(new Result(Set.of(1), 1)) == false) {
                        failures.add(
                            String.format(Locale.ROOT, "%s order=%s points=%s: expected [1] but got %s", kind.type(), order, points, result)
                        );
                    }
                }
            }
        }
        assertEquals(List.of(), failures);
    }

    public void testParentWithoutSortFieldValueNeverMatches() throws IOException {
        final List<Block> blocks = List.of(new Block(0, 10L, 1), new Block(1, null, 1), new Block(2, 20L, 1), new Block(3, 30L, 2));
        final List<String> failures = new ArrayList<>();
        for (final Kind kind : KINDS) {
            for (final String order : List.of("asc", "desc")) {
                for (final String missing : List.of("_first", "_last")) {
                    for (final boolean points : List.of(false, true)) {
                        if (points && kind.supportsPoints() == false) {
                            continue;
                        }
                        final Result inRange = search(
                            kind,
                            SortRole.PRIMARY,
                            points,
                            order,
                            missing,
                            "min",
                            blocks,
                            Set.of(),
                            20L,
                            20L,
                            true,
                            true
                        );
                        final Result everything = search(
                            kind,
                            SortRole.PRIMARY,
                            points,
                            order,
                            missing,
                            "min",
                            blocks,
                            Set.of(),
                            0L,
                            100L,
                            true,
                            true
                        );
                        if (inRange.equals(new Result(Set.of(2), 1)) == false
                            || everything.equals(new Result(Set.of(0, 2, 3), 3)) == false) {
                            failures.add(
                                String.format(
                                    Locale.ROOT,
                                    "%s order=%s missing=%s points=%s: got %s for [20, 20] and %s for [0, 100]",
                                    kind.type(),
                                    order,
                                    missing,
                                    points,
                                    inRange,
                                    everything
                                )
                            );
                        }
                    }
                }
            }
        }
        assertEquals(List.of(), failures);
    }

    public void testQueriedFieldIsNotThePrimarySortField() throws IOException {
        final List<Block> blocks = List.of(new Block(0, 10L, 1), new Block(1, 20L, 1), new Block(2, 30L, 1));
        final List<String> failures = new ArrayList<>();
        for (final Kind kind : KINDS) {
            for (final SortRole role : SortRole.values()) {
                if (role == SortRole.PRIMARY) {
                    continue;
                }
                for (final String order : List.of("asc", "desc")) {
                    for (final boolean points : List.of(false, true)) {
                        if (points && kind.supportsPoints() == false) {
                            continue;
                        }
                        final Result result = search(kind, role, points, order, "_last", "min", blocks, Set.of(), 20L, 20L, true, true);
                        if (result.equals(new Result(Set.of(1), 1)) == false) {
                            failures.add(
                                String.format(
                                    Locale.ROOT,
                                    "%s role=%s order=%s points=%s: expected [1] but got %s",
                                    kind.type(),
                                    role,
                                    order,
                                    points,
                                    result
                                )
                            );
                        }
                    }
                }
            }
        }
        assertEquals(List.of(), failures);
    }

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
                assertEquals(role == SortRole.PRIMARY && withNestedField == false, context.canUseIndexSortRangeQuery("field"));
                final boolean expectWrapper = role == SortRole.PRIMARY && withNestedField == false;
                assertEquals(
                    String.format(Locale.ROOT, "kind=%s role=%s nested=%s query=%s", kind.type(), role, withNestedField, query),
                    expectWrapper,
                    query instanceof IndexSortSortedNumericDocValuesRangeQuery
                );
            }
        }
    }

    private Result search(
        Kind kind,
        SortRole role,
        boolean points,
        String order,
        String missing,
        String mode,
        List<Block> blocks,
        Set<Integer> flushAfter,
        Long lower,
        Long upper,
        boolean includeLower,
        boolean includeUpper
    ) throws IOException {
        final MapperService mapperService = createMapperService(indexSettings(role, order, missing, mode), mappings(kind, points, true));
        final Sort indexSort = new IndexSortConfig(mapperService.getIndexSettings()).buildIndexSort(
            mapperService::fieldType,
            (ft, searchLookup) -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "")).build(null, null)
        );
        final IndexWriterConfig config = new IndexWriterConfig(null).setParentField(Engine.ROOT_DOC_FIELD_NAME);
        if (indexSort != null) {
            config.setIndexSort(indexSort);
        }
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (final Block block : blocks) {
                writer.addDocuments(mapperService.documentMapper().parse(source(Integer.toString(block.id()), b -> {
                    if (block.value() != null) {
                        b.field("field", kind.value().apply(block.value()));
                    }
                    b.field("other", role == SortRole.SECONDARY ? 0 : block.id());
                    b.field("block_id", block.id());
                    if (block.children() > 0) {
                        b.startArray("tags");
                        for (int child = 0; child < block.children(); child++) {
                            b.startObject().field("k", "a").endObject();
                        }
                        b.endArray();
                    }
                }, null)).docs());
                if (flushAfter.contains(block.id())) {
                    writer.flush();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                final IndexSearcher searcher = newSearcher(reader);
                final SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher);
                final RangeQueryBuilder range = new RangeQueryBuilder("field");
                if (lower != null) {
                    range.from(kind.value().apply(lower), includeLower);
                }
                if (upper != null) {
                    range.to(kind.value().apply(upper), includeUpper);
                }
                final Query rootDocsInRange = new BooleanQuery.Builder().add(range.toQuery(context), BooleanClause.Occur.FILTER)
                    .add(Queries.newNonNestedFilter(IndexVersion.current()), BooleanClause.Occur.FILTER)
                    .build();
                final Set<Integer> ids = new TreeSet<>();
                for (final ScoreDoc hit : searcher.search(rootDocsInRange, Integer.MAX_VALUE).scoreDocs) {
                    ids.add(searcher.storedFields().document(hit.doc).getField("block_id").numericValue().intValue());
                }
                return new Result(ids, searcher.count(rootDocsInRange));
            }
        }
    }

    private static Settings indexSettings(SortRole role, String order, String missing, String mode) {
        final List<String> sortFields = switch (role) {
            case PRIMARY -> List.of("field");
            case SECONDARY -> List.of("other", "field");
            case NOT_A_SORT_FIELD -> List.of("other");
            case UNSORTED -> List.of();
        };
        final Settings.Builder settings = Settings.builder().put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), randomBoolean());
        if (sortFields.isEmpty() == false) {
            settings.putList("index.sort.field", sortFields)
                .putList("index.sort.order", sortFields.stream().map(f -> order).toList())
                .putList("index.sort.missing", sortFields.stream().map(f -> missing).toList())
                .putList("index.sort.mode", sortFields.stream().map(f -> mode).toList());
        }
        return settings.build();
    }

    private static XContentBuilder mappings(Kind kind, boolean points, boolean withNestedField) throws IOException {
        return mapping(b -> {
            b.startObject("field").field("type", kind.type());
            if (kind.supportsPoints()) {
                b.field("index", points);
            }
            b.endObject();
            b.startObject("other").field("type", "long").endObject();
            b.startObject("block_id").field("type", "integer").field("store", true).endObject();
            if (withNestedField) {
                b.startObject("tags").field("type", "nested");
                b.startObject("properties").startObject("k").field("type", "keyword").endObject().endObject();
                b.endObject();
            }
        });
    }
}
