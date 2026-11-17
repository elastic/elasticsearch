/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractFlattenedColumnarArrayOrderTestCase extends MapperServiceTestCase {

    protected static final String FIELD = "crew";

    private static final String SEQUENCE = "sequence";

    private static final List<IndexMode> STRICT_COLUMNAR_MODES = Arrays.stream(IndexMode.values())
        .filter(IndexMode::isStrictColumnar)
        .toList();

    protected enum LeafArrays {
        EXACT("exact"),
        DEFAULT(null);

        private final String setting;

        LeafArrays(String setting) {
            this.setting = setting;
        }
    }

    protected record IndexedLeaf(MapperService mapperService, LeafReaderContext leaf, int[] sequenceByDoc) {}

    protected final void forEachStrictColumnarMode(CheckedConsumer<IndexMode, Exception> body) throws Exception {
        for (final IndexMode mode : STRICT_COLUMNAR_MODES) {
            try {
                body.accept(mode);
            } catch (AssertionError | Exception e) {
                throw new AssertionError("indexMode=" + mode.getName() + ": " + e, e);
            }
        }
    }

    protected final void forEachStrictColumnarModeAndLeafArrays(CheckedBiConsumer<IndexMode, LeafArrays, Exception> body) throws Exception {
        for (final LeafArrays leafArrays : LeafArrays.values()) {
            forEachStrictColumnarMode(mode -> {
                try {
                    body.accept(mode, leafArrays);
                } catch (AssertionError | Exception e) {
                    throw new AssertionError("leafArrays=" + leafArrays + ": " + e, e);
                }
            });
        }
    }

    protected final MapperService flattenedMapperService(IndexMode mode) throws IOException {
        return flattenedMapperService(mode, LeafArrays.EXACT);
    }

    protected final MapperService flattenedMapperService(IndexMode mode, LeafArrays leafArrays) throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build();
        return createMapperService(settings, mapping(b -> {
            b.startObject(FIELD).field("type", "flattened");
            b.field("ignore_above", Integer.MAX_VALUE);
            if (leafArrays.setting != null) {
                b.field("preserve_leaf_arrays", leafArrays.setting);
            }
            b.endObject();
        }));
    }

    protected final void withIndexedDocument(
        MapperService mapperService,
        CheckedConsumer<XContentBuilder, IOException> document,
        CheckedConsumer<IndexedLeaf, IOException> check
    ) throws IOException {
        withIndexedDocuments(mapperService, List.of(document), check);
    }

    protected final void withIndexedDocuments(
        MapperService mapperService,
        List<CheckedConsumer<XContentBuilder, IOException>> documents,
        CheckedConsumer<IndexedLeaf, IOException> check
    ) throws IOException {
        withLuceneIndex(mapperService, iw -> {
            for (int i = 0; i < documents.size(); i++) {
                final CheckedConsumer<XContentBuilder, IOException> document = documents.get(i);
                final CheckedConsumer<XContentBuilder, IOException> withTimestamp = b -> {
                    if (mapperService.mappingLookup().getFieldType(DataStreamTimestampFieldMapper.DEFAULT_PATH) != null) {
                        b.field(DataStreamTimestampFieldMapper.DEFAULT_PATH, "2026-01-01T00:00:00.000Z");
                    }
                    document.accept(b);
                };
                final LuceneDocument luceneDocument = mapperService.documentMapper().parse(source(withTimestamp)).rootDoc();
                luceneDocument.add(new StoredField(SEQUENCE, i));
                iw.addDocument(luceneDocument);
            }
            iw.forceMerge(1);
        }, reader -> {
            assertEquals(1, reader.leaves().size());
            final LeafReaderContext leaf = reader.leaves().get(0);
            final StoredFields storedFields = leaf.reader().storedFields();
            final int[] sequenceByDoc = new int[leaf.reader().maxDoc()];
            for (int doc = 0; doc < sequenceByDoc.length; doc++) {
                sequenceByDoc[doc] = storedFields.document(doc).getField(SEQUENCE).numericValue().intValue();
            }
            check.accept(new IndexedLeaf(mapperService, leaf, sequenceByDoc));
        });
    }
}
