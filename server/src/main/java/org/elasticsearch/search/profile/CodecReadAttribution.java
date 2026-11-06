/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.Scope;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.LongConsumer;

/**
 * The {@link Scope} that gives each codec reader its recorder and builds the {@link CodecReads} of the work. The search
 * profile and the ES|QL profile both use it. This is the one description of how codec reads reach a profile.
 *
 * <p><b>Availability and activation.</b> The feature flag {@link ProfileSelection#CODEC_READ_PROFILING_FEATURE_FLAG}
 * controls availability. A request activates codec read profiling with {@code profile}: {@code true}, or the object form
 * that holds a {@link ProfileSelection}. {@code SearchService} and the ES|QL {@code ComputeService} call
 * {@link ProfileSelection#ifEnabled} for a profiled request. A null result means that codec reads are not profiled, and
 * no scope is bound.
 *
 * <p><b>Recorders.</b> A frontend binds a scope with {@link Scopes#bind} around the code that opens readers. A reader is
 * a doc values reader opened for one field of one segment. An instrumented codec asks the bound scope for a recorder
 * once, when it opens the reader, and the reader keeps that recorder for its whole life. In the search profile,
 * {@code CodecReadProfiling} binds a scope for each call that builds a scorer or counts, and that scope creates an
 * instance when the first reader opens. The ES|QL profile binds one instance for each
 * {@code ValuesSourceReaderOperator}, and sets the {@link #partition} and the {@link #leaf} before it opens the readers
 * of a segment.
 *
 * <p><b>Totals.</b> An instance has one parent collector for each codec. Every reader of that codec records into it,
 * directly or through a detail collector. The codec totals therefore never depend on the selection.
 * {@link ProfileCollector#detail} explains why the timers of the parent collector measure the same calls with or
 * without details.
 *
 * <p><b>Selection and keys.</b> A reader gets a detail collector only when the selection reports its segment or its
 * field. Each reader has at most one detail. The key of a detail is:
 * <ul>
 *   <li>(segment, codec) for a reader of a selected segment whose field is not reported;</li>
 *   <li>(field, codec) for a reader of a reported field in a segment that is not selected;</li>
 *   <li>(segment, field) for a reader of a reported field in a selected segment. One codec reads such a pair.</li>
 * </ul>
 * A segment name is unique in one shard only, so the segment key is {@code <partition>/<segment>} when a partition is
 * set. The search profile and the ES|QL profile set the partition to an identifier of the shard. A selection names a
 * segment by its Lucene name, which matches in every shard, or by its key, which matches in one shard.
 *
 * <p><b>Views.</b> The {@code segments} and {@code fields} views are sums of disjoint details. {@link CodecReads} adds
 * them when it renders: a segment is the sum of the details with that segment, and a field is the sum of the details
 * with that field. No view counts a reader twice.
 *
 * <p><b>Descriptions.</b> A selected segment reports its index and shard when its leaf names them. Under
 * {@code "segments": "_all"} it also reports the rest of its description, such as its document counts, its index sort
 * and a summary of its field infos. A reported field reports its {@code field_info} and the {@code column} facts of
 * the codec only under {@code "fields": "_all"}. When two readers of one detail state a fact differently, the fact is a
 * conflict and is not reported. When two readers describe a field differently, only the keys they agree on are kept.
 *
 * <p><b>Memory.</b> The memory grows with the number of details. This
 * class calls {@code charge} before it allocates what it keeps for a detail or a segment context. The detail collector
 * reserves its own memory through {@link ProfileCollector.Reservations}, which call the same {@code charge}. The search
 * profile charges the request circuit breaker until the searcher closes. The ES|QL profile charges the driver's circuit
 * breaker until the operator closes. When a charge throws, the allocation does not happen and the reader does not
 * open. The amounts are estimates, not a bound on heap use. The parent collectors are not charged: there is one for each
 * codec.
 *
 * <p><b>No instrumented codec.</b> When no codec reader asks for a recorder, this class creates no collector,
 * {@link #snapshot} returns {@link CodecReads#EMPTY}, and the profile renders no codec reads.
 *
 * <p>An instance has one writer at a time, like the collectors it hands out.
 */
public final class CodecReadAttribution implements Scope {

    /** Charges nothing, for a frontend that has no circuit breaker to charge. */
    public static final LongConsumer NO_CHARGE = bytes -> {};

    private static final long MAP_ENTRY_BYTES = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 1
    );
    /** The bytes this class keeps for one detail: its key, its map entry, its holder and its field description. */
    public static final long PAIR_BYTES = RamUsageEstimator.shallowSizeOfInstance(Pair.class) + RamUsageEstimator.shallowSizeOfInstance(
        Detail.class
    ) + RamUsageEstimator.shallowSizeOfInstance(CodecReads.FieldContext.class) + MAP_ENTRY_BYTES;

    private final ProfileSelection selection;
    private final LongConsumer charge;
    private final ProfileCollector.Reservations reservations;
    private final Map<String, ProfileCollector> aggregates = new TreeMap<>();
    private final Map<Pair, Detail> details = new HashMap<>();
    /** The context of each selected segment that was read; null when its leaf could not describe it. */
    private final Map<String, CodecReads.SegmentContext> segments = new HashMap<>();
    @Nullable
    private String partition;
    /** The leaf whose readers are being opened; null when not known. */
    @Nullable
    private LeafReader leaf;

    /**
     * @param selection the segments and fields that get details
     * @param charge    called with the bytes of a detail before they are allocated; it can throw to refuse them
     */
    public CodecReadAttribution(ProfileSelection selection, LongConsumer charge) {
        this.selection = selection;
        this.charge = charge;
        this.reservations = charge::accept;
    }

    /**
     * Binds this scope on the current thread until the returned value is closed. Closing restores the previous binding.
     */
    public Releasable bind() {
        final Scopes.Binding binding = Scopes.bind(this);
        return binding::close;
    }

    /**
     * Sets the shard identifier that qualifies the segment keys of the next readers, or null for no qualifier. Readers
     * that are already open keep their recorder.
     */
    public void partition(@Nullable String partition) {
        this.partition = partition;
    }

    /**
     * Sets the leaf of the next readers, or null when it is not known. The leaf describes the segment and the field of a
     * detail. When no partition is set, the shard of the leaf also qualifies the segment key used to match the
     * selection. A leaf that is not the segment the codec names describes nothing.
     */
    public void leaf(@Nullable LeafReader leaf) {
        this.leaf = leaf;
    }

    @Override
    public MetricRecorder metrics(String codec, String segment, String field, Map<String, Boolean> facts) {
        final ProfileCollector aggregate = aggregates.computeIfAbsent(codec, c -> ProfileCollector.create(ProfileCollector.Mode.ELAPSED));
        final String key = partition == null ? segment : partition + "/" + segment;
        final boolean segmentSelected = selection.selectsSegment(segment, partition != null ? key : qualifiedByLeaf(segment));
        final boolean fieldReported = selection.selectsField(field) || (segmentSelected && selection.breaksDown(field));
        if (segmentSelected == false && fieldReported == false) {
            return aggregate;
        }
        final boolean pairOnly = segmentSelected && fieldReported;
        final Pair pair = new Pair(segmentSelected ? key : null, fieldReported ? field : null, pairOnly ? null : codec);
        final boolean describesField = selection.describesFields() && fieldReported;
        final SegmentReader segmentReader = leaf == null ? null : Lucene.tryUnwrapSegmentReader(leaf);
        final boolean described = segmentReader != null && segmentReader.getSegmentName().equals(segment);
        final Detail existing = details.get(pair);
        if (existing != null) {
            if (existing.codec.equals(codec) == false) {
                throw new IllegalStateException(
                    "field [" + field + "] of segment [" + key + "] is read by codec [" + existing.codec + "] and by [" + codec + "]"
                );
            }
            if (describesField) {
                existing.state(facts);
                if (segmentSelected == false && existing.describe(describedField(described, segmentReader, field))) {
                    charge.accept(AGREED_DESCRIPTION_BYTES);
                }
            }
            return existing.collector;
        }
        charge.accept(PAIR_BYTES + (describesField ? factBytes(facts) : 0));
        if (segmentSelected && segments.containsKey(key) == false) {
            final CodecReads.SegmentContext context = described
                ? describeSegment(segmentReader, leaf, selection.countNestedDocs() && selection.describesSegments())
                : null;
            if (context != null) {
                charge.accept(bytesOf(context));
            }
            segments.put(key, context);
        }
        final Detail created = new Detail(codec, aggregate.detail(reservations));
        if (describesField) {
            created.state(facts);
            created.describe(describedField(described, segmentReader, field));
        }
        details.put(pair, created);
        return created.collector;
    }

    /** The segment key {@code <index>:<shard>/<segment>} from the shard of the leaf, or null when the shard is not known. */
    @Nullable
    private String qualifiedByLeaf(String segment) {
        final ShardId shardId = leaf == null ? null : ShardUtils.extractShardId(leaf);
        return shardId == null ? null : shardId.getIndexName() + ":" + shardId.id() + "/" + segment;
    }

    /** The field infos of the leaf for {@code field}, or null when the leaf is not the segment or has no such field. */
    @Nullable
    private CodecReads.FieldContext describedField(boolean described, @Nullable SegmentReader segmentReader, String field) {
        final FieldInfo fieldInfo = described ? leaf.getFieldInfos().fieldInfo(field) : null;
        return fieldInfo == null ? null : describeField(fieldInfo, segmentReader.getSegmentInfo().info.getIndexSort());
    }

    /**
     * Describes the segment from data in memory. The only exception is the count of root documents of a segment with
     * blocks: it reads the parent field once, and only when {@code countNestedDocs} is true.
     */
    static CodecReads.SegmentContext describeSegment(SegmentReader segmentReader, LeafReader leaf, boolean countNestedDocs) {
        final FieldInfos fieldInfos = leaf.getFieldInfos();
        int docValuesFields = 0;
        final Map<String, Integer> formats = new TreeMap<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                docValuesFields++;
                final String format = fieldInfo.getAttribute(PerFieldDocValuesFormat.PER_FIELD_FORMAT_KEY);
                if (format != null) {
                    formats.merge(format, 1, Integer::sum);
                }
            }
        }
        final SegmentInfo info = segmentReader.getSegmentInfo().info;
        final Integer rootDocs;
        if (info.getHasBlocks() == false) {
            rootDocs = leaf.maxDoc();
        } else if (countNestedDocs && fieldInfos.getParentField() != null) {
            rootDocs = countRootDocs(leaf, fieldInfos.getParentField());
        } else {
            rootDocs = null;
        }
        final Sort indexSort = info.getIndexSort();
        final List<CodecReads.SortKey> keys = new ArrayList<>();
        if (indexSort != null) {
            for (SortField sortField : indexSort.getSort()) {
                keys.add(describeSortKey(sortField));
            }
        }
        final ShardId shardId = ShardUtils.extractShardId(leaf);
        return new CodecReads.SegmentContext(
            shardId == null ? null : shardId.getIndexName(),
            shardId == null ? null : shardId.id(),
            leaf.maxDoc(),
            leaf.numDocs(),
            rootDocs,
            info.getCodec().getName(),
            info.getVersion().toString(),
            info.getUseCompoundFile(),
            keys,
            new CodecReads.FieldInfosSummary(
                fieldInfos.size(),
                docValuesFields,
                formats,
                fieldInfos.hasNorms(),
                fieldInfos.hasPointValues(),
                fieldInfos.hasVectorValues(),
                fieldInfos.getSoftDeletesField(),
                fieldInfos.getParentField()
            )
        );
    }

    /**
     * Counts the documents that have the parent field. Lucene adds it to every top-level document, deleted ones
     * included. Returns null when the leaf has no doc values for the field.
     */
    @Nullable
    private static Integer countRootDocs(LeafReader leaf, String parentField) {
        try {
            final NumericDocValues parents = leaf.getNumericDocValues(parentField);
            if (parents == null) {
                return null;
            }
            int count = 0;
            while (parents.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                count++;
            }
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Describes one key of an index sort in the terms of the {@code index.sort.*} settings. */
    static CodecReads.SortKey describeSortKey(SortField sortField) {
        final String type;
        final String mode;
        if (sortField instanceof SortedNumericSortField numeric) {
            type = numeric.getNumericType().name();
            mode = numeric.getSelector().name().toLowerCase(Locale.ROOT);
        } else if (sortField instanceof SortedSetSortField sortedSet) {
            type = SortField.Type.STRING.name();
            mode = sortedSet.getSelector().name().toLowerCase(Locale.ROOT);
        } else {
            type = sortField.getType().name();
            mode = null;
        }
        return new CodecReads.SortKey(
            sortField.getField(),
            type,
            sortField.getReverse() ? "desc" : "asc",
            mode,
            missing(sortField.getMissingValue(), type, sortField.getReverse())
        );
    }

    /**
     * Returns where a document without a value sorts. Elasticsearch stores {@code _first} and {@code _last} as a sentinel:
     * a string marker, or the smallest or largest value of the type. The smallest value sorts first in ascending order
     * and last in descending order. Any other value is returned as a string. Returns null when no missing value is set.
     */
    @Nullable
    static String missing(@Nullable Object missing, String type, boolean reverse) {
        if (missing == null) {
            return null;
        }
        if (missing == SortField.STRING_FIRST) {
            return "first";
        }
        if (missing == SortField.STRING_LAST) {
            return "last";
        }
        final boolean smallest = switch (missing) {
            case Long value when value == Long.MIN_VALUE -> true;
            case Integer value when value == Integer.MIN_VALUE -> true;
            case Double value when value == Double.NEGATIVE_INFINITY || value == -Double.MAX_VALUE -> true;
            case Float value when value == Float.NEGATIVE_INFINITY || value == -Float.MAX_VALUE -> true;
            default -> false;
        };
        final boolean largest = switch (missing) {
            case Long value when value == Long.MAX_VALUE -> true;
            case Integer value when value == Integer.MAX_VALUE -> true;
            case Double value when value == Double.POSITIVE_INFINITY || value == Double.MAX_VALUE -> true;
            case Float value when value == Float.POSITIVE_INFINITY || value == Float.MAX_VALUE -> true;
            default -> false;
        };
        if (smallest || largest) {
            return smallest != reverse ? "first" : "last";
        }
        return missing.toString();
    }

    static CodecReads.FieldContext describeField(FieldInfo info, @Nullable Sort indexSort) {
        Integer sortPosition = null;
        if (indexSort != null) {
            final SortField[] keys = indexSort.getSort();
            for (int position = 0; position < keys.length; position++) {
                if (info.name.equals(keys[position].getField())) {
                    sortPosition = position;
                    break;
                }
            }
        }
        final boolean hasVectors = info.getVectorDimension() > 0;
        return new CodecReads.FieldContext(
            info.number,
            info.getDocValuesType().name(),
            info.getDocValuesGen(),
            info.docValuesSkipIndexType().name(),
            info.getIndexOptions().name(),
            info.getAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY),
            info.hasNorms(),
            info.omitsNorms(),
            info.hasTermVectors(),
            info.hasPayloads(),
            info.getPointDimensionCount(),
            info.getPointIndexDimensionCount(),
            info.getPointNumBytes(),
            info.getVectorDimension(),
            hasVectors ? info.getVectorEncoding().name() : null,
            hasVectors ? info.getVectorSimilarityFunction().name() : null,
            info.isSoftDeletesField(),
            info.isParentField(),
            sortPosition
        );
    }

    /**
     * Estimates the bytes a segment context keeps: its records, the map of formats, the sort keys, the strings created
     * for it and its map entry. Field names, codec names, format names and enum names are shared with the reader and
     * are not counted.
     */
    public static long bytesOf(CodecReads.SegmentContext context) {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(CodecReads.SegmentContext.class) + RamUsageEstimator.shallowSizeOfInstance(
            CodecReads.FieldInfosSummary.class
        ) + RamUsageEstimator.shallowSizeOfInstance(TreeMap.class) + (context.fieldInfos().docValuesFormats().size() + 1L) * MAP_ENTRY_BYTES
            + RamUsageEstimator.sizeOf(context.luceneVersion()) + RamUsageEstimator.shallowSizeOf(context.indexSort().toArray());
        if (context.shard() != null) {
            bytes += RamUsageEstimator.shallowSizeOfInstance(Integer.class);
        }
        for (CodecReads.SortKey key : context.indexSort()) {
            bytes += RamUsageEstimator.shallowSizeOfInstance(CodecReads.SortKey.class);
            if (key.missing() != null) {
                bytes += RamUsageEstimator.sizeOf(key.missing());
            }
        }
        return bytes;
    }

    /** Returns the counts so far. A detail is present once a reader opened with it, even when it counted nothing. */
    public CodecReads snapshot() {
        if (aggregates.isEmpty()) {
            return CodecReads.EMPTY;
        }
        final Map<String, Snapshot> totals = new TreeMap<>();
        aggregates.forEach((codec, collector) -> totals.put(codec, collector.snapshot()));
        final List<CodecReads.Detail> snapshots = new ArrayList<>(details.size());
        details.forEach(
            (pair, detail) -> snapshots.add(
                new CodecReads.Detail(pair.segment, pair.field, detail.codec, detail.fieldInfo(), detail.facts, detail.collector.snapshot())
            )
        );
        final Map<String, CodecReads.SegmentContext> described = new TreeMap<>();
        segments.forEach((segment, context) -> {
            if (context != null) {
                described.put(segment, context);
            }
        });
        return new CodecReads(
            totals,
            snapshots,
            described,
            selection.reportsFields(),
            selection.describesSegments(),
            selection.describesFields()
        );
    }

    /** Returns the number of details. */
    public int detailCount() {
        return details.size();
    }

    /**
     * The key of a detail. {@code segment} is null when the segment is not selected. {@code field} is null when the field
     * is not reported. {@code codec} is null when both are set, because one codec reads a (segment, field) pair.
     */
    private record Pair(@Nullable String segment, @Nullable String field, @Nullable String codec) {}

    /**
     * The bytes a detail keeps when two readers describe its field differently: a map of the keys they agree on, with
     * at most one entry for each rendered key.
     */
    static final long AGREED_DESCRIPTION_BYTES = RamUsageEstimator.shallowSizeOfInstance(java.util.LinkedHashMap.class)
        + (long) CodecReads.FieldContext.class.getRecordComponents().length * MAP_ENTRY_BYTES;

    /** Estimates the bytes of the facts of a detail: a map with one entry for each fact. */
    static long factBytes(Map<String, Boolean> facts) {
        return facts.isEmpty() ? 0 : RamUsageEstimator.shallowSizeOfInstance(TreeMap.class) + facts.size() * MAP_ENTRY_BYTES;
    }

    /** The state of one detail: the codec that reads it, its detail collector and its descriptions. */
    private static final class Detail {
        private final String codec;
        private final ProfileCollector collector;
        /** The first description of the field. It is used while every reader describes the field the same way. */
        @Nullable
        private CodecReads.FieldContext context;
        /** The keys the descriptions agree on, after two of them differ. Then {@link #context} is not used. */
        @Nullable
        private Map<String, Object> agreed;
        private Map<String, CodecReads.Fact> facts = Map.of();

        private Detail(String codec, ProfileCollector collector) {
            this.codec = codec;
            this.collector = collector;
        }

        /**
         * Records how a reader describes the field. When descriptions differ, only the keys they agree on are kept.
         *
         * @return true when this is the first description that differs, which allocates the map of agreed keys
         */
        private boolean describe(@Nullable CodecReads.FieldContext described) {
            if (described == null) {
                return false;
            }
            if (agreed != null) {
                agreed = CodecReads.agreed(agreed, described.render());
                return false;
            }
            if (context == null) {
                context = described;
                return false;
            }
            if (context.equals(described)) {
                return false;
            }
            agreed = CodecReads.agreed(context.render(), described.render());
            return true;
        }

        /** The rendered keys that all descriptions agree on, or null when no reader described the field. */
        @Nullable
        private Map<String, Object> fieldInfo() {
            return agreed != null ? agreed : context == null ? null : context.render();
        }

        /** Records the facts a reader states. A fact that two readers state differently becomes a conflict. */
        private void state(Map<String, Boolean> stated) {
            boolean unchanged = true;
            for (Map.Entry<String, Boolean> fact : stated.entrySet()) {
                unchanged &= facts.get(fact.getKey()) == CodecReads.Fact.of(fact.getValue());
            }
            if (unchanged) {
                return;
            }
            final Map<String, CodecReads.Fact> combined = new TreeMap<>(facts);
            stated.forEach((name, value) -> combined.merge(name, CodecReads.Fact.of(value), CodecReads.Fact::and));
            facts = combined;
        }
    }
}
