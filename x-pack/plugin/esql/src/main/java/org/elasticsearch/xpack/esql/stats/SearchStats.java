/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Map;

/**
 * Interface for determining information about fields in the index.
 * This is used by the optimizer to make decisions about how to optimize queries.
 */
public interface SearchStats {
    SearchStats EMPTY = new EmptySearchStats();

    boolean exists(FieldName field);

    boolean isIndexed(FieldName field);

    boolean hasDocValues(FieldName field);

    /**
     * Whether every shard here can query {@code field}, read as {@code type}: see
     * {@link SearchContextStats#isQueryable(SearchExecutionContext, String, DataType, boolean)}. At least one must hold the field.
     */
    default boolean isQueryable(FieldName field, DataType type, boolean comparesValuesAsRead) {
        return hasDocValues(field);
    }

    /**
     * Whether a shard here reads {@code field} as the key of a {@code flattened} field, a {@code keyword} whatever the type of the
     * field other indices map.
     */
    default boolean readsKey(FieldName field) {
        return false;
    }

    boolean hasExactSubfield(FieldName field);

    long count();

    long count(FieldName field);

    long count(FieldName field, BytesRef value);

    Object min(FieldName field);

    Object max(FieldName field);

    /**
     * The smallest and largest value of {@code field}, as the segment metadata (points or a doc values skipper) reports them,
     * for the case where that answer is exactly what a scan of the field would give.
     * <p>
     * Unlike {@link #min} and {@link #max}, which are estimates good enough to choose between plans, this is exact. It is
     * therefore {@code null}, meaning "ask the data instead", unless all of the following hold:
     * <ul>
     *   <li>the field is a {@code date}, {@code date_nanos}, {@code long}, {@code integer}, {@code short} or {@code byte}, which
     *       every shard that maps it reads as the same ES|QL type ({@code integer}, {@code short} and {@code byte} all read as
     *       {@code integer}), with doc values and with points or a doc values skipper;</li>
     *   <li>no segment has deleted documents: the metadata still covers a deleted document, and document level security hides
     *       documents the same way;</li>
     *   <li>no shard may hold the field outside of its mapping, where a scan would still read it;</li>
     *   <li>no shard applies a filter to its queries, such as an index alias filter, which the segments do not reflect.</li>
     * </ul>
     * Multi-valued fields are fine: the metadata covers every value, as do the minimum and maximum of a multi-valued field.
     */
    default ExactMinMax exactMinMax(FieldName field) {
        return null;
    }

    /**
     * The bounds {@link #exactMinMax} reports. Dates are the milliseconds or nanoseconds since the epoch.
     *
     * @param hasValues {@code false} when no document holds a value, which makes {@code MIN} and {@code MAX} {@code null}
     * @param type      the ES|QL type that every shard reads the field as, not the mapping type: {@code integer}, {@code short} and
     *                  {@code byte} mappings are all {@code INTEGER}. A caller can tell whether it reads the field as that type without
     *                  converting the values. {@code null} if no shard maps the field.
     */
    record ExactMinMax(boolean hasValues, long min, long max, DataType type) {
        public static ExactMinMax noValues(DataType type) {
            return new ExactMinMax(false, 0L, 0L, type);
        }

        public ExactMinMax {
            assert hasValues || (min == 0L && max == 0L) : "bounds of a field without values must be zero";
            assert min <= max;
        }
    }

    boolean isSingleValue(FieldName field);

    boolean canUseEqualityOnSyntheticSourceDelegate(FieldName name, String value);

    /**
     * Whether every index this reads holds no empty string for {@code field}, because one given at index time was
     * read as a null. A comparison against an empty string is then a question about whether the field holds a
     * value at all.
     */
    default boolean emptyStringReadsAsNull(FieldName field) {
        return false;
    }

    /**
     * Do all fields with the matching name answer a query over the values their doc values hold? Such a query matches a
     * document's value whole, which is what a predicate over the value means, so one can be pushed to Lucene for a
     * field whose index holds no exact form of it - a {@code text} field keeping its values in a column.
     */
    default boolean hasValueQueries(FieldName field) {
        return false;
    }

    /**
     * Do all fields with the matching name support this loader config?
     */
    boolean supportsLoaderConfig(FieldName name, BlockLoaderFunctionConfig config, MappedFieldType.FieldExtractPreference preference);

    /**
     * Returns the value for a field if it's a constant (eg. a constant_keyword with only one value for the involved indices).
     * NULL if the field is not a constant.
     */
    default String constantValue(FieldName name) {
        return null;
    }

    /**
     * Whether a document of any shard behind these stats may hold, in {@code _source}, a field that the shard's mapping does
     * not declare. When this returns {@code false}, {@code unmapped_fields="LOAD_ALL"} has nothing to discover on this node.
     * See {@code UnmappedFields#mayExist()} for what decides it.
     */
    default boolean mayHoldUnmappedFields() {
        return true;
    }

    /**
     * Whether {@code name} may be an unmapped field of a document of a shard behind these stats that does not map it. A
     * key of the value of a field a shard maps is not one on that shard. See
     * {@code UnmappedFields#mayExist(String)}.
     */
    default boolean mayHoldUnmappedField(FieldName name) {
        return true;
    }

    /**
     * Returns the mapped field type for the given field name, or null if the field is not found.
     */
    default MappedFieldType fieldType(FieldName name) {
        return null;
    }

    /**
     * Returns the target shards and their index metadata.
     */
    Map<ShardId, IndexMetadata> targetShards();

    default boolean canPartitionByTsidPrefix() {
        return false;
    }

    /**
     * When there are no search stats available, for example when there are no search contexts, we have static results.
     */
    record EmptySearchStats() implements SearchStats {

        @Override
        public boolean exists(FieldName field) {
            return false;
        }

        @Override
        public boolean isIndexed(FieldName field) {
            return false;
        }

        @Override
        public boolean hasDocValues(FieldName field) {
            return false;
        }

        @Override
        public boolean hasExactSubfield(FieldName field) {
            return false;
        }

        @Override
        public boolean supportsLoaderConfig(
            FieldName name,
            BlockLoaderFunctionConfig config,
            MappedFieldType.FieldExtractPreference preference
        ) {
            return false;
        }

        @Override
        public long count() {
            return 0;
        }

        @Override
        public long count(FieldName field) {
            return 0;
        }

        @Override
        public long count(FieldName field, BytesRef value) {
            return 0;
        }

        @Override
        public Object min(FieldName field) {
            return null;
        }

        @Override
        public Object max(FieldName field) {
            return null;
        }

        @Override
        public boolean isSingleValue(FieldName field) {
            return true;
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(FieldName name, String value) {
            return false;
        }

        @Override
        public Map<ShardId, IndexMetadata> targetShards() {
            return Map.of();
        }
    }

    /**
     * A default implementat that throws {@link UnsupportedOperationException} on all methods. Implemetors can override only the methods
     * they <i>know</i> would be called.
     */
    abstract class UnsupportedSearchStats implements SearchStats {
        @Override
        public boolean exists(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isIndexed(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasDocValues(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasExactSubfield(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsLoaderConfig(
            FieldName name,
            BlockLoaderFunctionConfig config,
            MappedFieldType.FieldExtractPreference preference
        ) {
            return false;
        }

        @Override
        public long count() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(FieldName field, BytesRef value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object min(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object max(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isSingleValue(FieldName field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(FieldName name, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<ShardId, IndexMetadata> targetShards() {
            throw new UnsupportedOperationException();
        }
    }
}
