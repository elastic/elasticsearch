/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

/**
 * Marker interface for {@link Attribute}s whose values are <em>synthesized by the engine</em> and
 * have no physical presence in the underlying source data — they are not columns in any file,
 * document, or index segment, and cannot be evaluated by a format-level scan.
 * <p>
 * Today the only implementation is {@link ExternalMetadataAttribute} ({@code _file.path},
 * {@code _file.name}, {@code _file.size}, etc.), which is materialized as a per-file constant
 * block by {@code VirtualColumnIterator}. Future synthetic attributes (Hive partition columns
 * surfaced as virtual columns, computed table-level columns, etc.) should also implement this
 * interface so format-level pushdown rules pick them up automatically.
 * <p>
 * <b>Why a marker rather than a name check.</b> The conventional {@code _file.*} prefix is a
 * surface convention; user-facing renames (and even internal aliasing) can change the visible
 * name. Type-based identification stays correct under any rename and survives serialization.
 * <p>
 * <b>Why a marker rather than a base class.</b> The single Java inheritance slot under
 * {@link TypedAttribute} is already spent by each concrete attribute (e.g.
 * {@link ExternalMetadataAttribute} and a hypothetical future {@code PartitionAttribute} that
 * needs to extend {@link FieldAttribute} for Lucene type metadata). The interface composes
 * orthogonally with whichever base each subclass needs and carries no behavior of its own —
 * adding a base would lock the hierarchy without buying any shared state. Same idiom as
 * {@code SurrogateExpression}, {@code OptionalArgument}, and similar capability tags in the
 * expression tree.
 * <p>
 * <b>Relationship to {@link MetadataAttribute}.</b> {@code MetadataAttribute} models <em>real</em>
 * Elasticsearch document metadata ({@code _id}, {@code _index}, {@code _score}, ...) — values the
 * index actually stores or computes per-document. Format-level pushdown to external file formats
 * (Parquet, ORC) cannot evaluate either of these, so the shared
 * {@code PushdownPredicates.isVirtualColumn} helper treats both {@code MetadataAttribute} and any
 * {@code VirtualAttribute} as virtual for pushdown purposes — but the two concepts stay distinct
 * here so other consumers can disambiguate (real-but-unscanable metadata vs engine-synthesized).
 */
public interface VirtualAttribute {}
