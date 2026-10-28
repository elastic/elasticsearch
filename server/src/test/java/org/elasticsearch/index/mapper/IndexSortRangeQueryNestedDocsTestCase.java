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
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;

abstract class IndexSortRangeQueryNestedDocsTestCase extends MapperServiceTestCase {

    record Kind(String type, LongFunction<Object> value, boolean supportsPoints, boolean usesIndexSortRangeQuery) {}

    record Block(int id, Long value, int children) {}

    record Result(Set<Integer> ids, int count) {}

    enum SortRole {
        PRIMARY,
        SECONDARY,
        NOT_A_SORT_FIELD,
        UNSORTED
    }

    static final List<Kind> KINDS = List.of(
        new Kind("date", value -> value, true, true),
        new Kind("date_nanos", value -> value, true, true),
        new Kind("long", value -> value, true, true),
        new Kind("integer", value -> value, true, true),
        new Kind("short", value -> value, true, true),
        new Kind("byte", value -> value, true, true),
        new Kind("keyword", value -> String.format(Locale.ROOT, "v%03d", value), false, false)
    );

    static Kind kind(String type) {
        return KINDS.stream().filter(kind -> kind.type().equals(type)).findFirst().orElseThrow();
    }

    final Result search(
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

    static Settings indexSettings(SortRole role, String order, String missing, String mode) {
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

    static XContentBuilder mappings(Kind kind, boolean points, boolean withNestedField) throws IOException {
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
