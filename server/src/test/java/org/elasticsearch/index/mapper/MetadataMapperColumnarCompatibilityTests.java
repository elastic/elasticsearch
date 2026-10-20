/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Columnar ↔ x-content compatibility tests for the metadata mappers implemented on the
 * {@code columnar_mappers} branch. See {@link AbstractColumnarMapperCompatibilityTestCase} for
 * the test harness and the rationale for synthetic source + recovery-disabled as the base settings.
 *
 * <p>Coverage: {@link ProvidedIdFieldMapper} ({@code mode=document}/{@code columnar}),
 * {@link SourceFieldMapper} (no-op and synthetic-recovery branches),
 * {@link VersionFieldMapper}, {@link SeqNoFieldMapper} ({@code POINTS_AND_DOC_VALUES},
 * {@code DOC_VALUES_ONLY}, {@code disable_sequence_numbers}),
 * {@link RoutingFieldMapper} ({@code doc_values=true} and {@code doc_values=false}),
 * and {@link IgnoredFieldMapper} ({@code ignore_above}-driven ignored values).
 */
public class MetadataMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    /** Base settings builder: synthetic source + recovery source disabled (see class Javadoc). */
    private static Settings.Builder syntheticSourceSettingsBuilder() {
        return Settings.builder()
            // Synthetic source: no stored _source; SourceFieldMapper produces nothing on either path.
            .put("index.mapping.source.mode", "synthetic")
            // Disable recovery source: prevents _recovery_source fields on the x-content path that
            // the columnar path cannot yet produce (SourceFieldMapper.supportsColumnarParse would be false).
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false);
    }

    private static Settings syntheticSourceSettings() {
        return syntheticSourceSettingsBuilder().build();
    }

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /** {@code _routing doc_values=true}: routing lands in sorted doc values; no {@code _field_names} divergence. */
    public void testRoutingDocValues() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(RoutingFieldMapper.NAME).field("doc_values", true).endObject()),
            syntheticSourceSettings(),
            batch("no routing - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("with routing - single doc", 1L, doc("doc2", "my-route", 200L, "{}")),
            // Mixed batch: doc 0 has no routing (SPARSE column null entry), docs 1-2 have routing.
            batch(
                "mixed routing batch",
                2L,
                doc("batch-1", null, 300L, 1L, "{}"),
                doc("batch-2", "route-a", 301L, 2L, "{}"),
                doc("batch-3", "route-b", 302L, 3L, "{}")
            )
        );
    }

    /**
     * {@code _routing doc_values=false}: both paths produce a {@code _field_names/_routing} indexed
     * entry so that exists queries on {@code _routing} work for indices without routing doc values.
     */
    public void testRoutingWithoutDocValues() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(RoutingFieldMapper.NAME).field("doc_values", false).endObject()),
            syntheticSourceSettings(),
            batch("with routing - doc_values=false", 1L, doc("doc1", "my-route", 100L, "{}")),
            batch(
                "mixed routing batch - doc_values=false",
                2L,
                doc("batch-1", null, 300L, 1L, "{}"),
                doc("batch-2", "route-a", 301L, 2L, "{}"),
                doc("batch-3", "route-b", 302L, 3L, "{}")
            )
        );
    }

    /**
     * Synthetic recovery ({@code index.recovery.use_synthetic_source=true}): both paths write
     * {@code _recovery_source_size} as a {@code NumericDocValuesField}. Recovery source is
     * deliberately enabled here (unlike other tests) to exercise this branch of
     * {@link SourceFieldMapper#preColumnarParse}.
     */
    public void testSourceSyntheticRecovery() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.mapping.source.mode", "synthetic")
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
            .build();
        assertColumnarMatchesXContent(
            topMapping(b -> {}),
            settings,
            batch("empty source - single doc", 1L, doc("doc1", 100L, "{}")),
            // TODO: use realistic non-empty source once a user data mapper supports the columnar
            // path — empty source avoids content fields that only x-content produces today.
            batch("empty source batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"), doc("batch-3", 103L, "{}"))
        );
    }

    /** {@code _id mode=document}: stored {@code StringField} on both paths. */
    public void testIdDocumentMode() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(IdFieldMapper.NAME).field("mode", "document").endObject()),
            syntheticSourceSettings(),
            batch("document mode - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("document mode - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"), doc("batch-3", 103L, "{}"))
        );
    }

    /** {@code _id mode=columnar}: {@code ColumnarIdField.TYPE} (BINARY doc values + indexed, not stored) on both paths. */
    public void testIdColumnarMode() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(IdFieldMapper.NAME).field("mode", "columnar").endObject()),
            syntheticSourceSettings(),
            batch("columnar id - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("columnar id - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"))
        );
    }

    /** {@code _seq_no index_options=DOC_VALUES_ONLY}: DV-only field on both paths (no BKD point). */
    public void testSeqNoDocValuesOnly() throws IOException {
        final Settings settings = syntheticSourceSettingsBuilder().put(
            IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(),
            SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY
        ).build();
        assertColumnarMatchesXContent(
            topMapping(b -> {}),
            settings,
            batch("doc_values_only - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("doc_values_only - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"))
        );
    }

    /**
     * {@code disable_sequence_numbers=true}: exercises the {@code sequenceNumbersDisabled()} branch
     * in {@link SeqNoFieldMapper#postColumnarParse}. Produced fields are identical to
     * {@code DOC_VALUES_ONLY}; the code path differs.
     */
    public void testSeqNoDisabled() throws IOException {
        // disable_sequence_numbers requires DOC_VALUES_ONLY; set both explicitly for clarity.
        final Settings settings = syntheticSourceSettingsBuilder().put(
            IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(),
            SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY
        ).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build();
        assertColumnarMatchesXContent(
            topMapping(b -> {}),
            settings,
            batch("seq_no_disabled - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("seq_no_disabled - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"))
        );
    }

    /** Single doc where the keyword value exceeds {@code ignore_above}: {@code _ignored} and the fallback column are emitted. */
    public void testSingleIgnoredField() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject("f").field("type", "keyword").field("ignore_above", 5).endObject()),
            columnarSettings(),
            batch("single ignored", 1L, doc("d1", 1L, "{\"f\":\"toolong\"}"))
        );
    }

    /** Two docs where no values are ignored: the {@code _ignored} accumulator stays empty. */
    public void testNoIgnoredFields() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject("f").field("type", "keyword").field("ignore_above", 5).endObject()),
            columnarSettings(),
            batch("no ignored", 1L, doc("d1", 1L, "{\"f\":\"ok\"}"), doc("d2", 2L, "{\"f\":\"fine\"}"))
        );
    }

    /**
     * Four docs with three keyword fields at {@code ignore_above=5}: per-doc ignored sets vary,
     * one doc is absent entirely, and doc 4 has all three fields ignored — exercising value
     * interning and the multi-valued {@code _ignored} array column.
     */
    public void testOverlappingAndMultiValued() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("field_a").field("type", "keyword").field("ignore_above", 5).endObject();
            b.startObject("field_b").field("type", "keyword").field("ignore_above", 5).endObject();
            b.startObject("field_c").field("type", "keyword").field("ignore_above", 5).endObject();
        }),
            columnarSettings(),
            batch(
                "overlapping multi-valued",
                1L,
                doc("d1", 1L, "{\"field_a\":\"toolong1\",\"field_b\":\"toolong2\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"field_a\":\"toolong3\"}"),
                doc("d4", 4L, "{\"field_b\":\"toolong4\",\"field_c\":\"toolong5\",\"field_a\":\"toolong6\"}")
            )
        );
    }

    /**
     * A columnar index with {@code columnar_stored} source. Synthetic recovery stays enabled, since {@code columnar_stored} requires it.
     */
    private static Settings columnarStoredSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .build();
    }

    public void testColumnarStoredSupportsColumnarParse() throws IOException {
        final MapperService mapperService = createMapperService(
            columnarStoredSettings(),
            mapping(b -> b.startObject("f").field("type", "keyword").endObject())
        );
        assertTrue(
            mapperService.mappingLookup()
                .getMapping()
                .getMetadataMapperByName(SourceFieldMapper.NAME)
                .supportsColumnarParse(mapperService.getIndexSettings())
        );
    }

    /**
     * The batch path rebuilds each row's {@code _source} from the mapped columns and must write the same {@code _ignored_source} blob
     * (and {@code .counts} companion) the row path writes in {@link SourceFieldMapper#postParse}.
     */
    public void testColumnarStoredSource() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("kwd").field("type", "keyword").endObject();
            b.startObject("num").field("type", "long").endObject();
            b.startObject("flag").field("type", "boolean").endObject();
        }),
            columnarStoredSettings(),
            batch("single doc", 1L, doc("d1", 1L, "{\"kwd\":\"hello\",\"num\":42,\"flag\":true}")),
            // A column mixing scalars and arrays is a UNION, which the number mapper does not map yet, so keep each batch uniform.
            batch(
                "arrays and absent docs",
                2L,
                doc("d1", 1L, "{\"kwd\":[\"b\",\"a\",\"b\"],\"num\":[3,1,2]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"kwd\":[\"x\"],\"num\":[5]}")
            ),
            batch(
                "scalars and absent docs",
                3L,
                doc("d1", 1L, "{\"flag\":false}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"kwd\":\"x\",\"num\":-7,\"flag\":true}")
            )
        );
    }

    /**
     * Values dropped by {@code ignore_above} are kept in a fallback column that the whole-document blob subsumes, so the batch has to
     * leave that column out as {@code postParse} does on the row path. Strict-columnar indices make {@code ignore_above} inert from
     * {@link IndexVersions#IGNORE_ABOVE_NO_OP_IN_COLUMNAR}, so this runs on the last version before it; at the current version no value
     * is ever dropped and there is no fallback column to prune.
     */
    public void testColumnarStoredSourceWithIgnoredValues() throws IOException {
        assertColumnarMatchesXContent(
            IndexVersionUtils.getPreviousVersion(IndexVersions.IGNORE_ABOVE_NO_OP_IN_COLUMNAR),
            mapping(b -> b.startObject("kwd").field("type", "keyword").field("ignore_above", 5).endObject()),
            columnarStoredSettings(),
            batch(
                "ignored values",
                1L,
                doc("d1", 1L, "{\"kwd\":\"toolong\"}"),
                doc("d2", 2L, "{\"kwd\":\"ok\"}"),
                doc("d3", 3L, "{\"kwd\":[\"ok\",\"toolong\"]}")
            )
        );
    }

    /**
     * A document that arrives as a row of a pre-built batch has no source bytes on its request, so {@code _recovery_source_size} has to
     * come from the batch row; recovery uses it to bound the memory of a batch of operations, and a size of {@code 0} would never
     * stop it. A document whose request does carry source keeps using the length of those bytes.
     */
    public void testRecoverySourceSizeOfRowBackedBatch() throws IOException {
        final MapperService mapperService = createMapperService(
            columnarStoredSettings(),
            mapping(b -> b.startObject("kwd").field("type", "keyword").endObject())
        );
        final BytesReference rowBacked = new BytesArray("{\"kwd\":\"row backed value\"}");
        final BytesReference withSource = new BytesArray("{\"kwd\":\"x\"}");
        final IndexRequest[] requests = new IndexRequest[] {
            new IndexRequest("test-index").id("row-backed").source(new BytesArray(new byte[0]), XContentType.JSON),
            new IndexRequest("test-index").id("with-source").source(withSource, XContentType.JSON) };
        final BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, requests[0]), new BulkItemRequest(1, requests[1]) };

        try (EscfEncoder encoder = new EscfEncoder(BytesRefRecycler.NON_RECYCLING_INSTANCE, false)) {
            encoder.addDocument(rowBacked, XContentType.JSON, 0);
            encoder.addDocument(withSource, XContentType.JSON, 0);
            try (
                EscfBatch escfBatch = encoder.buildPartition(0);
                BatchMappingContext ctx = new BatchMappingContext(
                    IndexOperationBatch.initFromBulk(items, 0, items.length, escfBatch, Engine.Operation.Origin.PRIMARY, 0L, 0L),
                    mapperService.mappingLookup(),
                    mapperService.getIndexSettings(),
                    new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY))
                )
            ) {
                mapperService.mappingLookup().getMapping().getMetadataMapperByName(SourceFieldMapper.NAME).preColumnarParse(ctx);

                final MappedColumns.RowCursor rows = ctx.columns().rowCursor();
                final long[] sizes = new long[items.length];
                for (int d = 0; d < items.length; d++) {
                    rows.advance();
                    sizes[d] = rows.fields()
                        .stream()
                        .filter(f -> f.name().equals(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME))
                        .findFirst()
                        .orElseThrow()
                        .numericValue()
                        .longValue();
                }
                assertThat(sizes[0], equalTo((long) escfBatch.row(0).sizeInBytes()));
                assertThat(sizes[0], greaterThan(0L));
                assertThat(sizes[1], equalTo((long) withSource.length()));
            }
        }
    }
}
