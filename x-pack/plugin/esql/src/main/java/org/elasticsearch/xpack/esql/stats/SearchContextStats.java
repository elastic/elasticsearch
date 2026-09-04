/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.DocCountFieldMapper.DocCountFieldType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper.TimestampFieldType;
import static org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import static org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;

/**
 * This class provides <code>SearchStats</code> from a list of <code>SearchExecutionContext</code>'s.
 * It contains primarily a cache of <code>FieldStats</code> which is dynamically updated as needed.
 * Each <code>FieldStats</code> contains <code>FieldConfig</code> information which is populated once at creation time.
 * The remaining statistics are lazily computed and cached only on demand.
 * This cache is not thread-safe.
 */
public class SearchContextStats implements SearchStats {

    private final List<SearchExecutionContext> contexts;

    private record FieldConfig(boolean exists, boolean hasExactSubfield, boolean indexed, boolean hasDocValues, MappedFieldType fieldType) {
        FieldConfig(boolean exists, boolean hasExactSubfield, boolean indexed, boolean hasDocValues) {
            this(exists, hasExactSubfield, indexed, hasDocValues, null);
        }
    }

    private static class FieldStats {
        private Long count;
        private Object min, max;
        private Boolean singleValue;
        private FieldConfig config;
    }

    private static final int CACHE_SIZE = 32;

    // simple non-thread-safe cache for avoiding unnecessary IO (which while fast is still I/O)
    private final Map<String, FieldStats> cache = new LinkedHashMap<>(CACHE_SIZE, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, FieldStats> eldest) {
            return size() > CACHE_SIZE;
        }
    };

    public static SearchStats from(List<SearchExecutionContext> contexts) {
        if (contexts == null || contexts.isEmpty()) {
            return SearchStats.EMPTY;
        }
        return new SearchContextStats(contexts);
    }

    private SearchContextStats(List<SearchExecutionContext> contexts) {
        this.contexts = contexts;
        assert contexts != null && contexts.isEmpty() == false;
    }

    private FieldStats makeFieldStats(String field) {
        var stat = new FieldStats();
        stat.config = makeFieldConfig(field);
        return stat;
    }

    private FieldConfig makeFieldConfig(String field) {
        boolean exists = false;
        boolean hasExactSubfield = true;
        boolean indexed = true;
        boolean hasDocValues = true;
        boolean mixedFieldType = false;
        MappedFieldType fieldType = null; // Extract the field type, it will be used by min/max later.
        // even if there are deleted documents, check the existence of a field
        // since if it's missing, deleted documents won't change that
        for (SearchExecutionContext context : contexts) {
            if (context.isMappedField(field)) {
                MappedFieldType type = context.getFieldType(field);
                if (fieldType == null) {
                    fieldType = type;
                } else if (mixedFieldType == false && fieldType.typeName().equals(type.typeName()) == false) {
                    mixedFieldType = true;
                }
                exists |= true;
                indexed &= type.indexType().hasDenseIndex();
                hasDocValues &= type.hasDocValues();
                hasExactSubfield &= type instanceof TextFieldMapper.TextFieldType t && t.canUseSyntheticSourceDelegateForQuerying();
            } else {
                indexed = false;
                hasDocValues = false;
                hasExactSubfield = false;
            }
            if (exists && indexed == false && hasDocValues == false && hasExactSubfield == false) {
                break;
            }
        }
        if (exists == false) {
            // if it does not exist on any context, no other settings are valid
            return new FieldConfig(false, false, false, false);
        } else {
            return new FieldConfig(exists, hasExactSubfield, indexed, hasDocValues, mixedFieldType ? null : fieldType);
        }
    }

    private boolean fastNoCacheFieldExists(String field) {
        for (SearchExecutionContext context : contexts) {
            if (context.isMappedField(field)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean exists(FieldName field) {
        var stat = cache.get(field.string());
        return stat != null ? stat.config.exists : fastNoCacheFieldExists(field.string());
    }

    @Override
    public boolean isIndexed(FieldName field) {
        return cache.computeIfAbsent(field.string(), this::makeFieldStats).config.indexed;
    }

    @Override
    public boolean hasDocValues(FieldName field) {
        return cache.computeIfAbsent(field.string(), this::makeFieldStats).config.hasDocValues;
    }

    @Override
    public boolean supportsLoaderConfig(
        FieldName name,
        BlockLoaderFunctionConfig config,
        MappedFieldType.FieldExtractPreference preference
    ) {
        if (config == null) {
            throw new UnsupportedOperationException("config must be provided");
        }
        for (SearchExecutionContext context : contexts) {
            MappedFieldType ft = context.getFieldType(name.string());
            if (ft == null) {
                /*
                 * Missing fields are always null no matter what we try to push so they
                 * should work, but we need this check here to prevent actually pushing
                 * to a LOOKUP JOIN. If the field comes from a LOOKUP JOIN  then it'll
                 * show up as missing here. And we can't push to those fields. Yet.
                 */
                return false;
            }
            if (ft.supportsBlockLoaderConfig(config, preference) == false) {
                // If any one field doesn't support the loader config we'll disable pushing the expression to the field
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasExactSubfield(FieldName field) {
        return cache.computeIfAbsent(field.string(), this::makeFieldStats).config.hasExactSubfield;
    }

    @Override
    public long count() {
        long count = 0;
        for (SearchExecutionContext context : contexts) {
            for (LeafReaderContext leafContext : context.searcher().getLeafContexts()) {
                LeafReader reader = leafContext.reader();
                if (reader.hasDeletions()) {
                    return -1L;
                }
                count += reader.numDocs();
            }
        }
        return count;
    }

    @Override
    public long count(FieldName field) {
        var stat = cache.computeIfAbsent(field.string(), this::makeFieldStats);
        if (stat.count != null) {
            return stat.count;
        }
        long count = 0;
        for (SearchExecutionContext context : contexts) {
            // Skip shards where this field is a dynamic sub-key of a flattened field rather
            // than an explicitly mapped field; those shards store the field's terms in Lucene
            // even though it is absent from the mapping, so counting without this guard
            // inflates the result.
            if (context.isMappedField(field.string()) == false) {
                continue;
            }
            for (LeafReaderContext leafContext : context.searcher().getLeafContexts()) {
                LeafReader reader = leafContext.reader();
                if (reader.hasDeletions()) {
                    // Can't use the count
                    return stat.count = -1L;
                }
                long c = countEntries(reader, field.string());
                if (c < 0) {
                    // Can't use the count
                    return stat.count = -1L;
                }
                count += c;
            }
        }
        return stat.count = count;
    }

    @Override
    public long count(FieldName field, BytesRef value) {
        Term term = new Term(field.string(), value);
        long count = 0;
        try {
            for (SearchExecutionContext context : contexts) {
                for (LeafReaderContext leafContext : context.searcher().getLeafContexts()) {
                    LeafReader reader = leafContext.reader();
                    if (reader.hasDeletions()) {
                        return -1L;
                    }
                    count += reader.docFreq(term);
                }
            }
        } catch (IOException ex) {
            throw new EsqlIllegalArgumentException("Cannot access data storage", ex);
        }
        return count;
    }

    @Override
    public Object min(FieldName field) {
        final var stat = cache.computeIfAbsent(field.string(), this::makeFieldStats);
        final MappedFieldType fieldType = stat.config.fieldType;
        if (fieldType instanceof DateFieldType == false) {
            return null;
        }
        if (stat.min == null) {
            final Long[] result = new Long[] { null };
            doWithFieldLeafReaders(field.string(), (ctxFieldType, reader) -> {
                final Long minValue = ctxFieldType.indexType().hasDocValuesSkipper()
                    ? docValuesSkipperMinValue(reader, field.string())
                    : pointMinValue(reader, field.string());
                result[0] = nullableMin(result[0], minValue);
                return true;
            });
            stat.min = result[0];
        }
        return stat.min;
    }

    @Override
    public Object max(FieldName field) {
        final var stat = cache.computeIfAbsent(field.string(), this::makeFieldStats);
        final MappedFieldType fieldType = stat.config.fieldType;
        if (fieldType instanceof DateFieldType == false) {
            return null;
        }
        if (stat.max == null) {
            final Long[] result = new Long[] { null };
            doWithFieldLeafReaders(field.string(), (ctxFieldType, reader) -> {
                final Long maxValue = ctxFieldType.indexType().hasDocValuesSkipper()
                    ? docValuesSkipperMaxValue(reader, field.string())
                    : pointMaxValue(reader, field.string());
                result[0] = nullableMax(result[0], maxValue);
                return true;
            });
            stat.max = result[0];
        }
        return stat.max;
    }

    private static Long nullableMin(final Long a, final Long b) {
        if (a == null) return b;
        if (b == null) return a;
        return Math.min(a, b);
    }

    private static Long nullableMax(final Long a, final Long b) {
        if (a == null) return b;
        if (b == null) return a;
        return Math.max(a, b);
    }

    // TODO: replace these helpers with a unified Lucene min/max API once https://github.com/apache/lucene/issues/15740 is resolved
    private static Long docValuesSkipperMinValue(final LeafReader reader, final String field) throws IOException {
        long value = DocValuesSkipper.globalMinValue(reader, field);
        return (value == Long.MAX_VALUE || value == Long.MIN_VALUE) ? null : value;
    }

    private static Long docValuesSkipperMaxValue(final LeafReader reader, final String field) throws IOException {
        long value = DocValuesSkipper.globalMaxValue(reader, field);
        return (value == Long.MAX_VALUE || value == Long.MIN_VALUE) ? null : value;
    }

    private static Long pointMinValue(final LeafReader reader, final String field) throws IOException {
        final byte[] minPackedValue = PointValues.getMinPackedValue(reader, field);
        return (minPackedValue != null && minPackedValue.length == 8) ? NumericUtils.sortableBytesToLong(minPackedValue, 0) : null;
    }

    private static Long pointMaxValue(final LeafReader reader, final String field) throws IOException {
        final byte[] maxPackedValue = PointValues.getMaxPackedValue(reader, field);
        return (maxPackedValue != null && maxPackedValue.length == 8) ? NumericUtils.sortableBytesToLong(maxPackedValue, 0) : null;
    }

    @Override
    public boolean isSingleValue(FieldName field) {
        String fieldName = field.string();
        var stat = cache.computeIfAbsent(fieldName, this::makeFieldStats);
        if (stat.singleValue == null) {
            // a missing field is trivially single-valued; otherwise every leaf must prove it
            stat.singleValue = stat.config.exists == false
                || doWithFieldLeafReaders(fieldName, (fieldType, reader) -> isSingleValueLeaf(fieldType, reader, fieldName));
        }
        return stat.singleValue;
    }

    private boolean isSingleValueLeaf(MappedFieldType fieldType, LeafReader reader, String name) throws IOException {
        // types that are always single value (and are accessible through instanceof)
        if (fieldType instanceof ConstantFieldType || fieldType instanceof DocCountFieldType || fieldType instanceof TimestampFieldType) {
            return true;
        }

        final String typeName = fieldType.typeName();
        if (typeName.equals(IdFieldMapper.NAME) || typeName.equals(SeqNoFieldMapper.NAME)) {
            return true;
        }

        if (fieldType instanceof DateFieldType || fieldType instanceof NumberFieldType) {
            if (fieldType.indexType().hasPoints()) {
                final PointValues values = reader.getPointValues(name);
                return values == null || values.size() == values.getDocCount();
            }
            if (fieldType.indexType().hasDocValuesSkipper()) {
                final DocValuesSkipper skipper = reader.getDocValuesSkipper(name);
                return skipper == null || skipper.maxValueCount() == 1;
            }
            return false;
        }

        if (fieldType instanceof KeywordFieldType keywordFieldType) {
            // NOTE: Terms cannot prove value cardinality for these keyword storage shapes.
            if (canUseKeywordTermsForDocValueCountEquality(keywordFieldType) == false) {
                return false;
            }
            final Terms terms = reader.terms(name);
            return terms == null || terms.getSumDocFreq() == terms.getDocCount();
        }

        // unsupported type - default to MV
        return false;
    }

    private static boolean canUseKeywordTermsForDocValueCountEquality(KeywordFieldType fieldType) {
        return fieldType.usesMultivaluedBinaryDocValues() == false && fieldType.indexType().hasTerms();
    }

    @Override
    public boolean canUseEqualityOnSyntheticSourceDelegate(FieldAttribute.FieldName name, String value) {
        for (SearchExecutionContext ctx : contexts) {
            MappedFieldType type = ctx.getFieldType(name.string());
            if (type == null) {
                return false;
            }
            if (type instanceof TextFieldMapper.TextFieldType t) {
                if (t.canUseSyntheticSourceDelegateForQueryingEquality(value) == false) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public String constantValue(FieldAttribute.FieldName name) {
        String val = null;
        for (SearchExecutionContext ctx : contexts) {
            MappedFieldType f = ctx.getFieldType(name.string());
            if (f == null) {
                return null;
            }
            if (f instanceof ConstantFieldType cf) {
                var fetcher = cf.valueFetcher(ctx, null);
                String thisVal = null;
                try {
                    // since the value is a constant, the doc _should_ be irrelevant
                    List<Object> vals = fetcher.fetchValues(null, -1, null);
                    Object objVal = vals.size() == 1 ? vals.get(0) : null;
                    // we are considering only string values for now, since this can return "strange" things,
                    // see IndexModeFieldType
                    thisVal = objVal instanceof String ? (String) objVal : null;
                } catch (IOException iox) {}

                if (thisVal == null) {
                    // Value not yet set
                    return null;
                }
                if (val == null) {
                    val = thisVal;
                } else if (thisVal.equals(val) == false) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return val;
    }

    @Override
    public MappedFieldType fieldType(FieldName field) {
        return cache.computeIfAbsent(field.string(), this::makeFieldStats).config.fieldType;
    }

    //
    // @see org.elasticsearch.search.query.QueryPhaseCollectorManager#shortcutTotalHitCount(IndexReader, Query)
    //
    private static long countEntries(IndexReader indexReader, String field) {
        long count = 0;
        try {
            for (LeafReaderContext context : indexReader.leaves()) {
                LeafReader reader = context.reader();
                FieldInfos fieldInfos = reader.getFieldInfos();
                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

                if (fieldInfo != null) {
                    if (fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                        // no shortcut possible: it's a text field, empty values are counted as no value.
                        return -1;
                    }
                    if (fieldInfo.getPointIndexDimensionCount() > 0) {
                        PointValues points = reader.getPointValues(field);
                        if (points != null) {
                            count += points.size();
                        }
                    } else if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                        Terms terms = reader.terms(field);
                        if (terms != null) {
                            count += terms.getSumTotalTermFreq();
                        }
                    } else {
                        return -1; // no shortcut possible for fields that are not indexed
                    }
                }
            }
        } catch (IOException ex) {
            throw new EsqlIllegalArgumentException("Cannot access data storage", ex);
        }
        return count;
    }

    private interface FieldLeafReaderTester {
        /**
         * Returns true if iteration should continue, false to stop early. The field type is the one
         * mapped by the context the leaf belongs to, so a decision is always made against the field
         * type of the shard that produced the leaf.
         */
        boolean test(MappedFieldType fieldType, LeafReader reader) throws IOException;
    }

    private boolean doWithFieldLeafReaders(String field, FieldLeafReaderTester tester) {
        try {
            for (SearchExecutionContext context : contexts) {
                if (context.isMappedField(field) == false) {
                    continue;
                }
                MappedFieldType fieldType = context.getFieldType(field);
                for (LeafReaderContext leafContext : context.searcher().getLeafContexts()) {
                    if (tester.test(fieldType, leafContext.reader()) == false) {
                        return false;
                    }
                }
            }
            return true;
        } catch (IOException ex) {
            throw new EsqlIllegalArgumentException("Cannot access data storage", ex);
        }
    }

    @Override
    public Map<ShardId, IndexMetadata> targetShards() {
        Map<ShardId, IndexMetadata> shards = Maps.newHashMapWithExpectedSize(contexts.size());
        for (SearchExecutionContext context : contexts) {
            IndexMetadata indexMetadata = context.getIndexSettings().getIndexMetadata();
            ShardId shardId = new ShardId(context.index(), context.getShardId());
            shards.putIfAbsent(shardId, indexMetadata);
        }
        return shards;
    }

    @Override
    public boolean canPartitionByTsidPrefix() {
        try {
            for (SearchExecutionContext context : contexts) {
                if (PartitionedDocValues.canPartitionByTsidPrefix(context.searcher()) == false) {
                    return false;
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("failed to read time-series partition", ex);
        }
        return true;
    }
}
