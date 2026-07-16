/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

/**
 * Compatibility test for the basic metadata mappers that have a columnar implementation on the
 * {@code columnar_mappers} branch.
 *
 * <p>Each scenario is run through both the columnar batch-mapping path
 * ({@link MetadataFieldMapper#preColumnarParse}/{@link MetadataFieldMapper#postColumnarParse}
 * + {@link org.elasticsearch.sourcebatch.MappedColumns#rowCursor()}) and the conventional
 * x-content parse path ({@link DocumentMapper#parse}), and the resulting per-document
 * {@link org.apache.lucene.index.IndexableField} sets are compared for equality.
 *
 * <h2>Index settings rationale</h2>
 * <ul>
 *   <li><strong>Synthetic source</strong> ({@code index.mapping.source.mode=synthetic}): with
 *       regular stored source, {@link SourceFieldMapper} would add a {@code _source} stored field
 *       on the x-content path but not on the columnar path (where it is unsupported). Synthetic
 *       source avoids this divergence.</li>
 *   <li><strong>Recovery source disabled</strong>
 *       ({@code indices.recovery.recovery_source.enabled=false}): with recovery source enabled
 *       (but not synthetic), the x-content path stores a {@code _recovery_source} field while
 *       {@link SourceFieldMapper#supportsColumnarParse} returns {@code false}, again producing
 *       a mismatch. Disabling recovery source makes both paths agree that no recovery source
 *       data is needed.</li>
 * </ul>
 * Together these settings ensure {@link SourceFieldMapper#supportsColumnarParse} is {@code true}
 * and both paths perform the same (no-op) work for {@code _source}.
 *
 * <h2>Routing variants</h2>
 * Two {@code _routing} mapping configurations are tested:
 * <ul>
 *   <li><strong>{@code doc_values=true}</strong> ({@link #testRoutingDocValues}): routing values
 *       land in sorted doc values on both paths; no {@code _field_names} entry is involved, so the
 *       paths produce identical fields.</li>
 *   <li><strong>{@code doc_values=false}</strong> ({@link #testRoutingWithoutDocValues}): both paths
 *       write a {@code StringField}, but when a routing value is present the x-content path also
 *       calls {@code context.addToFieldNames("_routing")} adding a {@code _field_names} entry.
 *       The columnar path cannot yet do so (no columnar {@code _field_names} plumbing). This test is
 *       therefore annotated {@code @AwaitsFix} until that gap is resolved
 *       (see the TODO in {@link RoutingFieldMapper#preColumnarParse(BatchMappingContext)}).</li>
 * </ul>
 *
 * <h2>Covered metadata mappers</h2>
 * <ul>
 *   <li>{@link ProvidedIdFieldMapper} — {@code _id}</li>
 *   <li>{@link VersionFieldMapper} — {@code _version}</li>
 *   <li>{@link SeqNoFieldMapper} — {@code _seq_no} and {@code _primary_term}</li>
 *   <li>{@link RoutingFieldMapper} — {@code _routing} (when a routing value is provided)</li>
 * </ul>
 * Other metadata mappers ({@link FieldNamesFieldMapper}, {@link DocCountFieldMapper},
 * {@link IgnoredFieldMapper}, {@link IgnoredSourceFieldMapper}, {@link IndexFieldMapper},
 * {@link IndexModeFieldMapper}, {@link NestedPathFieldMapper}) either produce no fields for
 * the empty-source documents used here, or are fully no-ops on both paths.
 *
 * @see AbstractColumnarMapperCompatibilityTestCase
 */
public class MetadataMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    /**
     * Index settings shared by all tests: synthetic source (no stored {@code _source}) and
     * recovery source disabled (prevents {@code _recovery_source} divergence).
     */
    private static Settings syntheticSourceSettings() {
        return Settings.builder()
            // Synthetic source: no stored _source; SourceFieldMapper produces nothing on either path.
            .put("index.mapping.source.mode", "synthetic")
            // Disable recovery source: prevents _recovery_source fields on the x-content path that
            // the columnar path cannot yet produce (SourceFieldMapper.supportsColumnarParse would be false).
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /**
     * Exercises _routing with {@code doc_values=true}: routing values land in sorted doc values
     * ({@code SortedDocValuesField.indexedField}) on both paths. No {@code _field_names} entry is
     * involved, so the two paths produce identical fields. This test covers:
     * <ul>
     *   <li>no-routing doc — exercises {@code _id}, {@code _version}, {@code _seq_no},
     *       {@code _primary_term}.</li>
     *   <li>with-routing doc — additionally exercises {@code _routing} (doc-values variant).</li>
     *   <li>mixed-routing batch — verifies {@link org.elasticsearch.sourcebatch.MappedColumns.RowCursor}
     *       position alignment across docs with distinct engine values.</li>
     * </ul>
     */
    public void testRoutingDocValues() throws IOException {
        assertColumnarMatchesXContent(
            // Configure _routing with doc_values=true so routing values land in sorted doc values
            // (no _field_names entry) rather than stored fields, ensuring parity with the columnar path.
            topMapping(b -> b.startObject(RoutingFieldMapper.NAME).field("doc_values", true).endObject()),
            syntheticSourceSettings(),
            // Exercises _id, _version, _seq_no, _primary_term.
            scenario("no routing - single doc", 1L, doc("doc1", 100L, "{}")),

            // Exercises _routing (doc_values=true) in addition to the base fields.
            scenario("with routing - single doc", 1L, doc("doc2", "my-route", 200L, "{}")),

            // Exercises RowCursor position alignment across docs and per-doc engine values.
            // Doc 0: no routing (routing column is SPARSE — null entry for doc 0).
            // Doc 1: routing present.
            // Doc 2: routing present, distinct version.
            scenario(
                "mixed routing batch",
                2L,
                doc("batch-1", null, 300L, 1L, "{}"),
                doc("batch-2", "route-a", 301L, 2L, "{}"),
                doc("batch-3", "route-b", 302L, 3L, "{}")
            )
        );
    }

    /**
     * Exercises _routing with {@code doc_values=false}: both paths write a {@code StringField}, but
     * when a routing value is set the x-content path also calls {@code context.addToFieldNames("_routing")}
     * adding a {@code _field_names} entry. The columnar path cannot yet do so; there is no columnar
     * {@code _field_names} plumbing (see the TODO in {@link RoutingFieldMapper#preColumnarParse}).
     * This test is expected to fail on the missing {@code _field_names} StringField until that gap
     * is closed in a follow-up PR.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/TODO")
    public void testRoutingWithoutDocValues() throws IOException {
        assertColumnarMatchesXContent(
            // _routing with doc_values=false: stored StringField on both paths, but x-content also
            // adds a _field_names entry when a routing value is present (columnar path cannot yet).
            topMapping(b -> b.startObject(RoutingFieldMapper.NAME).field("doc_values", false).endObject()),
            syntheticSourceSettings(),
            // With a routing value: x-content adds _field_names/_routing, columnar does not.
            scenario("with routing - doc_values=false", 1L, doc("doc1", "my-route", 100L, "{}"))
        );
    }
}
