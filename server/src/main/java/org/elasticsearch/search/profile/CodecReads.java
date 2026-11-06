/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Measurement;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TimerMeasurement;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * The codec reads of profiled work: the codec totals and the details the selection asks for. It is an immutable value.
 * Frontends merge it, send it between nodes and render it without access to a live collector.
 * {@link CodecReadAttribution} describes how the details are keyed and how the views are built.
 *
 * <p>Each measurement carries its metric definition, also on the wire, so a merge rejects two definitions of one metric.
 * Measurements merge under the rules of {@link Snapshot#merge}. A merge also fails when two codecs read one
 * (segment, field) pair, because only a codec bug can cause that.
 *
 * <p>Descriptions of segments and fields, and the facts a codec states about a column, are not measurements. When two
 * parts describe one segment differently, the segment description becomes a conflict. When two parts state a fact
 * differently, the fact becomes a conflict. A conflict stays a conflict through later merges and is not reported. A
 * field description keeps only the keys that all parts agree on. A part without a description does not change the
 * result.
 */
public final class CodecReads implements Writeable {

    public static final CodecReads EMPTY = new CodecReads(Map.of(), List.of(), Map.of(), false, false, false);

    /** The unit a timer's value is rendered with. */
    static final String NANOSECONDS = "ns";

    private static final Comparator<String> NULLS_FIRST = Comparator.nullsFirst(Comparator.naturalOrder());
    private static final Comparator<Detail> DETAIL_ORDER = Comparator.comparing(Detail::segment, NULLS_FIRST)
        .thenComparing(Detail::field, NULLS_FIRST)
        .thenComparing(Detail::codec);

    private static final byte COUNTER = 0;
    private static final byte TIMER = 1;

    private final Map<String, Snapshot> totals;
    private final List<Detail> details;
    private final Map<String, SegmentContext> segments;
    private final boolean fieldTotals;
    private final boolean segmentDescriptions;
    private final boolean fieldDescriptions;

    /**
     * @param totals              the codec totals, by codec name
     * @param details             the details; at most one for each key, and one codec for each (segment, field) pair
     * @param segments            the segment descriptions, by segment key; a segment that could not be described is
     *                            absent, and a segment whose descriptions conflict maps to null
     * @param fieldTotals         whether the {@code fields} view is reported
     * @param segmentDescriptions whether segment descriptions are reported beyond the index and shard
     * @param fieldDescriptions   whether field descriptions and column facts are reported
     */
    public CodecReads(
        Map<String, Snapshot> totals,
        List<Detail> details,
        Map<String, SegmentContext> segments,
        boolean fieldTotals,
        boolean segmentDescriptions,
        boolean fieldDescriptions
    ) {
        final List<Detail> sortedDetails = new ArrayList<>(details);
        sortedDetails.sort(DETAIL_ORDER);
        this.totals = Collections.unmodifiableMap(new TreeMap<>(totals));
        this.details = Collections.unmodifiableList(sortedDetails);
        this.segments = Collections.unmodifiableMap(new TreeMap<>(segments));
        this.fieldTotals = fieldTotals;
        this.segmentDescriptions = segmentDescriptions;
        this.fieldDescriptions = fieldDescriptions;
    }

    public CodecReads(StreamInput in) throws IOException {
        this(
            in.readMap(StreamInput::readString, CodecReads::readSnapshot),
            in.readCollectionAsList(Detail::new),
            in.readMap(StreamInput::readString, i -> i.readOptionalWriteable(SegmentContext::new)),
            in.readBoolean(),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(totals, StreamOutput::writeString, CodecReads::writeSnapshot);
        out.writeCollection(details);
        out.writeMap(segments, StreamOutput::writeString, StreamOutput::writeOptionalWriteable);
        out.writeBoolean(fieldTotals);
        out.writeBoolean(segmentDescriptions);
        out.writeBoolean(fieldDescriptions);
    }

    /**
     * Writes the measurements of {@code snapshot} in order, each with its full definition: identity, unit or timing
     * policy, description and, for a timer, its CPU time companion.
     */
    static void writeSnapshot(StreamOutput out, Snapshot snapshot) throws IOException {
        final List<Measurement> measurements = snapshot.measurements();
        out.writeVInt(measurements.size());
        for (Measurement measurement : measurements) {
            switch (measurement) {
                case CounterMeasurement counter -> {
                    out.writeByte(COUNTER);
                    out.writeString(counter.id().key());
                    out.writeEnum(counter.definition().unit());
                    out.writeString(counter.definition().description());
                    out.writeVLong(counter.value());
                }
                case TimerMeasurement timer -> {
                    final TimerMetric definition = timer.definition();
                    out.writeByte(TIMER);
                    out.writeString(definition.id().key());
                    out.writeEnum(definition.policy());
                    out.writeString(definition.description());
                    out.writeBoolean(definition.cpuTime() != null);
                    if (definition.cpuTime() != null) {
                        out.writeString(definition.cpuTime().id().key());
                        out.writeString(definition.cpuTime().description());
                    }
                    out.writeVLong(timer.elapsedNanos());
                    out.writeBoolean(timer.cpuNanos().isPresent());
                    if (timer.cpuNanos().isPresent()) {
                        out.writeVLong(timer.cpuNanos().getAsLong());
                    }
                    out.writeVLong(timer.sampledCalls());
                    out.writeVLong(timer.totalCalls());
                    out.writeBoolean(timer.estimated());
                }
            }
        }
    }

    /**
     * Reads what {@link #writeSnapshot} wrote and validates each definition and value as a local collector does. This
     * is the only place that parses a metric key.
     *
     * @throws IllegalStateException when a definition or a value is invalid
     */
    static Snapshot readSnapshot(StreamInput in) throws IOException {
        final int size = in.readVInt();
        final List<Measurement> measurements = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            final byte kind = in.readByte();
            final String key = in.readString();
            try {
                measurements.add(switch (kind) {
                    case COUNTER -> {
                        final CounterMetric definition = CounterMetric.of(
                            Metric.Id.parse(key),
                            in.readEnum(Metric.Unit.class),
                            in.readString()
                        );
                        yield new CounterMeasurement(definition, in.readVLong());
                    }
                    case TIMER -> {
                        TimerMetric definition = TimerMetric.of(Metric.Id.parse(key), in.readEnum(TimingPolicy.class), in.readString());
                        if (in.readBoolean()) {
                            final Metric.Id cpu = Metric.Id.parse(in.readString());
                            if (cpu.group().equals(definition.id().group()) == false
                                || cpu.component().equals(definition.id().component()) == false) {
                                throw new IllegalArgumentException(
                                    "the CPU time [" + cpu + "] is not in the group and component of its timer"
                                );
                            }
                            definition = definition.withCpuTime(cpu.name(), in.readString());
                        }
                        final long elapsed = in.readVLong();
                        final OptionalLong cpuNanos = in.readBoolean() ? OptionalLong.of(in.readVLong()) : OptionalLong.empty();
                        yield new TimerMeasurement(definition, elapsed, cpuNanos, in.readVLong(), in.readVLong(), in.readBoolean());
                    }
                    default -> throw new IllegalArgumentException("unknown measurement kind [" + kind + "]");
                });
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("received an invalid codec read profile measurement for [" + key + "]", e);
            }
        }
        return Snapshot.of(measurements);
    }

    /** The codec totals, ordered by codec name. */
    public Map<String, Snapshot> totals() {
        return totals;
    }

    /** The details, ordered by segment, then field, then codec, with null first. */
    public List<Detail> details() {
        return details;
    }

    /** Whether the {@code fields} view is reported. */
    public boolean fieldTotals() {
        return fieldTotals;
    }

    /** Whether segment descriptions are reported beyond the index and shard. */
    public boolean segmentDescriptions() {
        return segmentDescriptions;
    }

    /** Whether field descriptions and column facts are reported. */
    public boolean fieldDescriptions() {
        return fieldDescriptions;
    }

    /** The segment descriptions, by segment key. A key that maps to null is a segment whose descriptions conflict. */
    public Map<String, SegmentContext> segments() {
        return segments;
    }

    /** Whether no instrumented codec opened a reader while the work was profiled. */
    public boolean isEmpty() {
        return totals.isEmpty();
    }

    /**
     * Sums {@code parts}: the codec totals by codec and the details by key. Descriptions merge as the class
     * documentation says.
     *
     * @throws IllegalStateException when the parts disagree in a way that only a codec bug can cause
     */
    public static CodecReads merge(Collection<CodecReads> parts) {
        if (parts.size() == 1) {
            return parts.iterator().next();
        }
        final Map<String, List<Snapshot>> totalsByCodec = new TreeMap<>();
        final Map<DetailKey, List<Detail>> detailsByKey = new HashMap<>();
        final Map<String, SegmentContext> segments = new TreeMap<>();
        boolean fieldTotals = false;
        boolean segmentDescriptions = false;
        boolean fieldDescriptions = false;
        for (CodecReads part : parts) {
            fieldTotals |= part.fieldTotals;
            segmentDescriptions |= part.segmentDescriptions;
            fieldDescriptions |= part.fieldDescriptions;
            part.segments.forEach((segment, context) -> {
                if (segments.containsKey(segment) == false) {
                    segments.put(segment, context);
                } else if (Objects.equals(segments.get(segment), context) == false) {
                    segments.put(segment, null);
                }
            });
            part.totals.forEach((codec, snapshot) -> totalsByCodec.computeIfAbsent(codec, c -> new ArrayList<>()).add(snapshot));
            for (Detail detail : part.details) {
                detailsByKey.computeIfAbsent(DetailKey.of(detail), k -> new ArrayList<>()).add(detail);
            }
        }
        final Map<String, Snapshot> totals = new TreeMap<>();
        totalsByCodec.forEach((codec, snapshots) -> totals.put(codec, Snapshot.merge(snapshots)));
        final List<Detail> details = new ArrayList<>();
        for (List<Detail> same : detailsByKey.values()) {
            Detail merged = same.get(0);
            for (int i = 1; i < same.size(); i++) {
                merged = merged.plus(same.get(i));
            }
            details.add(merged);
        }
        return new CodecReads(totals, details, segments, fieldTotals, segmentDescriptions, fieldDescriptions);
    }

    /**
     * Renders the codec totals by codec name, and the metrics of each codec by component. The totals of two codecs are
     * never added, because each codec defines its own metrics. The totals count every reader of the codec.
     *
     * <p>With every segment selected, counters and the {@code total_calls} of each timer equal the sum over the
     * segments. The value of a timer equals that sum only when every call was a measured call, in the totals and in each
     * detail. Otherwise each value is an estimated time of its own calls, and the estimates do not add up.
     */
    public Map<String, Object> renderTotals() {
        final Map<String, Object> rendered = new LinkedHashMap<>();
        totals.forEach((codec, snapshot) -> rendered.put(codec, render(codec, snapshot)));
        return rendered;
    }

    /**
     * Renders the {@code segments} view by segment key. Each entry has the {@code index} and {@code shard} when known, the
     * rest of the segment description when segment descriptions are reported, and the {@code metrics} of all its
     * readers by codec. When fields are also selected, {@code fields} holds each selected field in the segment, with its
     * {@code metrics} and, when field descriptions are reported, its {@code field_info} and {@code column}. Empty when
     * no selected segment was read.
     */
    public Map<String, Object> renderSegments() {
        final Map<String, List<Detail>> bySegment = new LinkedHashMap<>();
        for (Detail detail : details) {
            if (detail.segment != null) {
                bySegment.computeIfAbsent(detail.segment, s -> new ArrayList<>()).add(detail);
            }
        }
        final Map<String, Object> rendered = new LinkedHashMap<>();
        bySegment.forEach((segment, parts) -> {
            final Map<String, Object> entry = new LinkedHashMap<>();
            final SegmentContext context = segments.get(segment);
            if (context != null) {
                if (segmentDescriptions) {
                    context.render(entry);
                } else {
                    context.renderIdentity(entry);
                }
            }
            entry.put("metrics", renderByCodec(parts));
            final Map<String, List<Detail>> byField = new LinkedHashMap<>();
            for (Detail detail : parts) {
                if (detail.field != null) {
                    byField.computeIfAbsent(detail.field, f -> new ArrayList<>()).add(detail);
                }
            }
            if (byField.isEmpty() == false) {
                final Map<String, Object> fields = new LinkedHashMap<>();
                byField.forEach((field, pair) -> fields.put(field, renderField(pair)));
                entry.put("fields", fields);
            }
            rendered.put(segment, entry);
        });
        return rendered;
    }

    /**
     * Renders the {@code fields} view by field name. Each entry has the {@code metrics} of the field over all segments,
     * by codec. When field descriptions are reported, it also has the {@code field_info} keys that all segments agree on
     * and the {@code column} facts that no segment contradicts. Empty when fields are not reported or no selected field
     * was read.
     */
    public Map<String, Object> renderFields() {
        if (fieldTotals == false) {
            return Map.of();
        }
        final Map<String, List<Detail>> byField = new TreeMap<>();
        for (Detail detail : details) {
            if (detail.field != null) {
                byField.computeIfAbsent(detail.field, f -> new ArrayList<>()).add(detail);
            }
        }
        final Map<String, Object> rendered = new LinkedHashMap<>();
        byField.forEach((field, parts) -> rendered.put(field, renderField(parts)));
        return rendered;
    }

    /** Renders the details of one field, in one segment or in many: the descriptions when reported, and the metrics. */
    private Map<String, Object> renderField(List<Detail> parts) {
        final Map<String, Object> entry = new LinkedHashMap<>();
        if (fieldDescriptions) {
            Map<String, Object> fieldInfo = null;
            final Map<String, Fact> facts = new TreeMap<>();
            for (Detail part : parts) {
                fieldInfo = agreed(fieldInfo, part.fieldInfo);
                part.facts.forEach((name, fact) -> facts.merge(name, fact, Fact::and));
            }
            if (fieldInfo != null && fieldInfo.isEmpty() == false) {
                entry.put("field_info", fieldInfo);
            }
            final Map<String, Object> column = new TreeMap<>();
            facts.forEach((name, fact) -> {
                if (fact != Fact.CONFLICT) {
                    column.put(name, fact == Fact.TRUE);
                }
            });
            if (column.isEmpty() == false) {
                entry.put("column", column);
            }
        }
        entry.put("metrics", renderByCodec(parts));
        return entry;
    }

    /**
     * Returns the keys that two field descriptions have with equal values, in the order of {@code left}. When one
     * description is null, returns the other one.
     */
    @Nullable
    static Map<String, Object> agreed(@Nullable Map<String, Object> left, @Nullable Map<String, Object> right) {
        if (left == null || right == null) {
            return left == null ? right : left;
        }
        final Map<String, Object> agreed = new LinkedHashMap<>();
        left.forEach((key, value) -> {
            if (Objects.equals(value, right.get(key))) {
                agreed.put(key, value);
            }
        });
        return agreed.size() == left.size() ? left : Collections.unmodifiableMap(agreed);
    }

    /** Renders the measurements of {@code parts}, summed by codec and ordered by codec name. */
    private static Map<String, Object> renderByCodec(List<Detail> parts) {
        final Map<String, List<Snapshot>> byCodec = new TreeMap<>();
        for (Detail part : parts) {
            byCodec.computeIfAbsent(part.codec, c -> new ArrayList<>()).add(part.reads);
        }
        final Map<String, Object> rendered = new LinkedHashMap<>();
        byCodec.forEach((codec, snapshots) -> rendered.put(codec, render(codec, Snapshot.merge(snapshots))));
        return rendered;
    }

    /**
     * Renders the measurements of {@code codec} by component, then by name. A metric whose group is the codec name is
     * under its component. A metric of another group is under {@code <group>.<component>}. A component has no dot, so
     * two metrics never share a place. A measured CPU time is under the name of its companion, with the calls of its
     * timer.
     */
    private static Map<String, Map<String, Object>> render(String codec, Snapshot snapshot) {
        final Map<String, Map<String, Object>> components = new TreeMap<>();
        for (Measurement measurement : snapshot.measurements()) {
            switch (measurement) {
                case CounterMeasurement counter -> place(components, codec, counter.id(), counter.value());
                case TimerMeasurement timer -> {
                    place(components, codec, timer.id(), renderTime(timer.elapsedNanos(), timer));
                    if (timer.cpuNanos().isPresent()) {
                        place(components, codec, timer.definition().cpuTime().id(), renderTime(timer.cpuNanos().getAsLong(), timer));
                    }
                }
            }
        }
        return components;
    }

    private static void place(Map<String, Map<String, Object>> components, String codec, Metric.Id id, Object value) {
        final String component = id.group().equals(codec) ? id.component() : id.group() + "." + id.component();
        components.computeIfAbsent(component, c -> new TreeMap<>()).put(id.name(), value);
    }

    /**
     * Renders a time with its {@code value} in nanoseconds, its {@code unit}, {@code estimated}, {@code total_calls} and,
     * for an estimated time, the number of measured calls as {@code sampled_calls}. A time is estimated when some calls
     * were not measured and count at the mean of the measured calls. A timer that measured every call is exact, whatever
     * its policy, and a merge of exact parts is exact. The call counts show how much of the work was measured, not how
     * accurate the estimate is.
     */
    private static Map<String, Object> renderTime(long nanos, TimerMeasurement timer) {
        final Map<String, Object> rendered = new LinkedHashMap<>();
        rendered.put("value", nanos);
        rendered.put("unit", NANOSECONDS);
        rendered.put("estimated", timer.estimated());
        rendered.put("total_calls", timer.totalCalls());
        if (timer.estimated()) {
            rendered.put("sampled_calls", timer.sampledCalls());
        }
        return rendered;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CodecReads other
            && totals.equals(other.totals)
            && details.equals(other.details)
            && segments.equals(other.segments)
            && fieldTotals == other.fieldTotals
            && segmentDescriptions == other.segmentDescriptions
            && fieldDescriptions == other.fieldDescriptions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totals, details, segments, fieldTotals, segmentDescriptions, fieldDescriptions);
    }

    @Override
    public String toString() {
        return "CodecReads[totals="
            + totals
            + ", details="
            + details
            + ", segments="
            + segments
            + ", fieldTotals="
            + fieldTotals
            + ", segmentDescriptions="
            + segmentDescriptions
            + ", fieldDescriptions="
            + fieldDescriptions
            + "]";
    }

    /** A fact a codec states about a column, combined over the parts of a detail. */
    public enum Fact {
        TRUE,
        FALSE,
        /** The parts disagree. A conflict stays a conflict through later merges and is not reported. */
        CONFLICT;

        static Fact of(boolean value) {
            return value ? TRUE : FALSE;
        }

        /** Combines the facts of two parts. */
        Fact and(Fact other) {
            return this == other ? this : CONFLICT;
        }
    }

    /**
     * The merge key of a detail: its segment and field, and its codec unless both are set, because one codec reads a
     * (segment, field) pair.
     */
    private record DetailKey(@Nullable String segment, @Nullable String field, @Nullable String codec) {
        static DetailKey of(Detail detail) {
            final boolean pair = detail.segment != null && detail.field != null;
            return new DetailKey(detail.segment, detail.field, pair ? null : detail.codec);
        }
    }

    /**
     * The measurements of one detail.
     *
     * @param segment   the segment key, or null when the segment is not selected
     * @param field     the field name, or null when the field is not reported
     * @param codec     the name of the doc values format that read it
     * @param fieldInfo the rendered {@code field_info} keys that the parts agree on, or null when field descriptions are
     *                  not reported or no part described the field
     * @param facts     the column facts by the names of the codec, such as {@code skip_index}; empty when the codec
     *                  stated none or field descriptions are not reported
     * @param reads     the measurements of the readers of the detail
     */
    public record Detail(
        @Nullable String segment,
        @Nullable String field,
        String codec,
        @Nullable Map<String, Object> fieldInfo,
        Map<String, Fact> facts,
        Snapshot reads
    ) implements Writeable {

        public Detail {
            assert segment != null || field != null : "a detail is of a segment, a field, or both";
            fieldInfo = fieldInfo == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(fieldInfo));
            facts = Collections.unmodifiableMap(new TreeMap<>(facts));
        }

        Detail(StreamInput in) throws IOException {
            this(
                in.readOptionalString(),
                in.readOptionalString(),
                in.readString(),
                in.readBoolean() ? in.readGenericMap() : null,
                in.readMap(StreamInput::readString, i -> i.readEnum(Fact.class)),
                readSnapshot(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(segment);
            out.writeOptionalString(field);
            out.writeString(codec);
            out.writeBoolean(fieldInfo != null);
            if (fieldInfo != null) {
                out.writeGenericMap(fieldInfo);
            }
            out.writeMap(facts, StreamOutput::writeString, StreamOutput::writeEnum);
            writeSnapshot(out, reads);
        }

        /**
         * Adds another part of the same detail.
         *
         * @throws IllegalStateException when two codecs read one (segment, field) pair, which only a codec bug can cause
         */
        Detail plus(Detail other) {
            if (codec.equals(other.codec) == false) {
                throw new IllegalStateException(
                    "field [" + field + "] of segment [" + segment + "] is read by codec [" + codec + "] and by [" + other.codec + "]"
                );
            }
            final Map<String, Fact> combinedFacts = new TreeMap<>(facts);
            other.facts.forEach((name, fact) -> combinedFacts.merge(name, fact, Fact::and));
            return new Detail(
                segment,
                field,
                codec,
                agreed(fieldInfo, other.fieldInfo),
                combinedFacts,
                Snapshot.merge(List.of(reads, other.reads))
            );
        }
    }

    /**
     * The description of a segment, read from the leaf when the first detail of the segment is created. It is not a
     * measurement.
     *
     * @param index         the name of the index the segment belongs to, as the node that holds the shard names it; null
     *                      when the reader does not say
     * @param shard         the number of the shard the segment belongs to; null when the reader does not say
     * @param maxDoc        the documents of the segment, deleted ones included
     * @param numDocs       the live documents the search could see, after deletes and any document level security
     * @param rootDocs      the top-level documents among {@code maxDoc}; the rest are nested. {@code maxDoc} when the
     *                      segment has no document blocks. Null when it has blocks that were not counted; counting
     *                      reads the parent field and needs {@code count_nested_docs}
     * @param luceneCodec   the name of the codec the segment was written with
     * @param luceneVersion the version of Lucene that wrote the segment
     * @param compoundFile  whether the segment's files are packed into a compound file
     * @param indexSort     the sort the segment's documents are in, empty when it has none
     * @param fieldInfos    what the segment's field infos hold as a whole
     */
    public record SegmentContext(
        @Nullable String index,
        @Nullable Integer shard,
        int maxDoc,
        int numDocs,
        @Nullable Integer rootDocs,
        String luceneCodec,
        String luceneVersion,
        boolean compoundFile,
        List<SortKey> indexSort,
        FieldInfosSummary fieldInfos
    ) implements Writeable {

        public SegmentContext {
            indexSort = List.copyOf(indexSort);
        }

        SegmentContext(StreamInput in) throws IOException {
            this(
                in.readOptionalString(),
                in.readOptionalVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readOptionalVInt(),
                in.readString(),
                in.readString(),
                in.readBoolean(),
                in.readCollectionAsList(SortKey::new),
                new FieldInfosSummary(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(index);
            out.writeOptionalVInt(shard);
            out.writeVInt(maxDoc);
            out.writeVInt(numDocs);
            out.writeOptionalVInt(rootDocs);
            out.writeString(luceneCodec);
            out.writeString(luceneVersion);
            out.writeBoolean(compoundFile);
            out.writeCollection(indexSort);
            fieldInfos.writeTo(out);
        }

        /** Renders the index and the shard of the segment, when known. */
        void renderIdentity(Map<String, Object> into) {
            if (index != null) {
                into.put("index", index);
            }
            if (shard != null) {
                into.put("shard", shard);
            }
        }

        void render(Map<String, Object> into) {
            renderIdentity(into);
            into.put("max_doc", maxDoc);
            into.put("num_docs", numDocs);
            if (rootDocs != null) {
                into.put("root_docs", rootDocs);
                into.put("nested_docs", maxDoc - rootDocs);
            }
            into.put("lucene_codec", luceneCodec);
            into.put("lucene_version", luceneVersion);
            into.put("compound_file", compoundFile);
            if (indexSort.isEmpty() == false) {
                final List<Map<String, Object>> keys = new ArrayList<>(indexSort.size());
                for (SortKey key : indexSort) {
                    keys.add(key.render());
                }
                into.put("index_sort", keys);
            }
            into.put("field_infos", fieldInfos.render());
        }
    }

    /**
     * One key of an index sort, in the terms of the {@code index.sort.*} settings.
     *
     * @param field   the field sorted on
     * @param type    the type of the sorted values, such as {@code STRING} or {@code LONG}
     * @param order   {@code asc} or {@code desc}
     * @param mode    which value of a multi-valued field sorts the document, {@code min} or {@code max}; null for a
     *                field that is not sorted by a selector
     * @param missing where a document without a value goes, {@code first} or {@code last}, or the raw value for a sort
     *                Elasticsearch did not configure; null when the sort sets none
     */
    public record SortKey(String field, String type, String order, @Nullable String mode, @Nullable String missing) implements Writeable {

        SortKey(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readString(), in.readOptionalString(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(field);
            out.writeString(type);
            out.writeString(order);
            out.writeOptionalString(mode);
            out.writeOptionalString(missing);
        }

        Map<String, Object> render() {
            final Map<String, Object> key = new LinkedHashMap<>();
            key.put("field", field);
            key.put("type", type);
            key.put("order", order);
            if (mode != null) {
                key.put("mode", mode);
            }
            if (missing != null) {
                key.put("missing", missing);
            }
            return key;
        }
    }

    /**
     * A summary of the field infos of a segment.
     *
     * @param count            the fields of the segment the search could see
     * @param docValuesCount   those of them that have doc values
     * @param docValuesFormats how many of those each doc values format serves, by format name; a field whose format is
     *                         not recorded is in {@code docValuesCount} only
     * @param hasNorms         whether a field has norms
     * @param hasPoints        whether a field has points
     * @param hasVectors       whether a field has vectors
     * @param softDeletesField the field that marks soft-deleted documents, or null when there is none
     * @param parentField      the field that marks the parent of a block of documents, or null when there is none
     */
    public record FieldInfosSummary(
        int count,
        int docValuesCount,
        Map<String, Integer> docValuesFormats,
        boolean hasNorms,
        boolean hasPoints,
        boolean hasVectors,
        @Nullable String softDeletesField,
        @Nullable String parentField
    ) implements Writeable {

        public FieldInfosSummary {
            docValuesFormats = Collections.unmodifiableMap(new TreeMap<>(docValuesFormats));
        }

        FieldInfosSummary(StreamInput in) throws IOException {
            this(
                in.readVInt(),
                in.readVInt(),
                in.readMap(StreamInput::readString, StreamInput::readVInt),
                in.readBoolean(),
                in.readBoolean(),
                in.readBoolean(),
                in.readOptionalString(),
                in.readOptionalString()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
            out.writeVInt(docValuesCount);
            out.writeMap(docValuesFormats, StreamOutput::writeString, StreamOutput::writeVInt);
            out.writeBoolean(hasNorms);
            out.writeBoolean(hasPoints);
            out.writeBoolean(hasVectors);
            out.writeOptionalString(softDeletesField);
            out.writeOptionalString(parentField);
        }

        Map<String, Object> render() {
            final Map<String, Object> rendered = new LinkedHashMap<>();
            rendered.put("count", count);
            rendered.put("doc_values_count", docValuesCount);
            rendered.put("doc_values_formats", docValuesFormats);
            rendered.put("has_norms", hasNorms);
            rendered.put("has_points", hasPoints);
            rendered.put("has_vectors", hasVectors);
            if (softDeletesField != null) {
                rendered.put("soft_deletes_field", softDeletesField);
            }
            if (parentField != null) {
                rendered.put("parent_field", parentField);
            }
            return rendered;
        }
    }

    /**
     * The field infos of one field in a segment, and the position of the field in the index sort. Lucene enums are
     * given by name.
     *
     * @param number                   the field's number in the segment
     * @param docValuesType            the kind of doc values, such as {@code BINARY}
     * @param docValuesGen             the generation of the doc values updates, -1 when they were never updated
     * @param docValuesSkipIndexType   the Lucene skip index of the doc values, {@code NONE} when they have none
     * @param indexOptions             what the inverted index holds for the field, {@code NONE} when it is not indexed
     * @param postingsFormat           the postings format of an indexed field, or null when it is not recorded
     * @param hasNorms                 whether the field has norms
     * @param omitsNorms               whether norms were turned off for the field
     * @param hasTermVectors           whether the field has term vectors
     * @param hasPayloads              whether the field's postings have payloads
     * @param pointDimensionCount      the dimensions of the field's points, 0 when it has none
     * @param pointIndexDimensionCount the dimensions of the points that are indexed
     * @param pointNumBytes            the bytes of one dimension of a point
     * @param vectorDimension          the dimension of the field's vectors, 0 when it has none
     * @param vectorEncoding           the encoding of the vectors, or null when the field has none
     * @param vectorSimilarity         the similarity of the vectors, or null when the field has none
     * @param softDeletesField         whether the field marks soft-deleted documents
     * @param parentField              whether the field marks the parent of a block of documents
     * @param sortPosition             the field's position in the index sort, or null when the index is not sorted on it
     */
    public record FieldContext(
        int number,
        String docValuesType,
        long docValuesGen,
        String docValuesSkipIndexType,
        String indexOptions,
        @Nullable String postingsFormat,
        boolean hasNorms,
        boolean omitsNorms,
        boolean hasTermVectors,
        boolean hasPayloads,
        int pointDimensionCount,
        int pointIndexDimensionCount,
        int pointNumBytes,
        int vectorDimension,
        @Nullable String vectorEncoding,
        @Nullable String vectorSimilarity,
        boolean softDeletesField,
        boolean parentField,
        @Nullable Integer sortPosition
    ) {

        Map<String, Object> render() {
            final Map<String, Object> rendered = new LinkedHashMap<>();
            rendered.put("number", number);
            rendered.put("doc_values_type", docValuesType);
            rendered.put("doc_values_gen", docValuesGen);
            rendered.put("doc_values_skip_index_type", docValuesSkipIndexType);
            rendered.put("index_options", indexOptions);
            if (postingsFormat != null) {
                rendered.put("postings_format", postingsFormat);
            }
            rendered.put("has_norms", hasNorms);
            rendered.put("omits_norms", omitsNorms);
            rendered.put("has_term_vectors", hasTermVectors);
            rendered.put("has_payloads", hasPayloads);
            rendered.put("point_dimension_count", pointDimensionCount);
            rendered.put("point_index_dimension_count", pointIndexDimensionCount);
            rendered.put("point_num_bytes", pointNumBytes);
            rendered.put("vector_dimension", vectorDimension);
            if (vectorEncoding != null) {
                rendered.put("vector_encoding", vectorEncoding);
            }
            if (vectorSimilarity != null) {
                rendered.put("vector_similarity", vectorSimilarity);
            }
            rendered.put("soft_deletes_field", softDeletesField);
            rendered.put("parent_field", parentField);
            rendered.put("sort_field", sortPosition != null);
            if (sortPosition != null) {
                rendered.put("sort_position", sortPosition);
            }
            return rendered;
        }
    }
}
