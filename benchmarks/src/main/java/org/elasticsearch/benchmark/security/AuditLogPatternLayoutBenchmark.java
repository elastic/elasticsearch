/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.security;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.impl.MutableLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.StringMapMessage;
import org.apache.logging.log4j.util.StringBuilderFormattable;
import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares two ways of producing the security audit JSON line, both mirroring the two phases {@code LoggingAuditTrail} goes
 * through per event - an <b>assembly</b> phase that stores field values, and a <b>render</b> phase that emits the JSON line:
 * <ul>
 *   <li>{@link Log4j} - the production approach: assemble a Log4j {@link StringMapMessage} and render it with a
 *       {@link PatternLayout} whose pattern carries one {@code %varsNotEmpty{, "field":"%enc{%map{field}}{JSON}"}} entry per
 *       field, matching the audit appender in {@code x-pack/plugin/core/src/main/config/log4j2.properties}.</li>
 *   <li>{@link SlotArray} - an alternative approach: assemble into a plain {@code String[]} slot array, each value routed to its
 *       fixed slot via a {@code name -> slot} map lookup then a positional store (no sorted insert), wrapped in a {@link Message}
 *       that is also {@link StringBuilderFormattable}, so a trivial {@code %m%n} layout invokes its {@code formatTo}
 *       to write the JSON straight into Log4j's buffer - no intermediate string, no {@code %map}/{@code %enc}/
 *       {@code %varsNotEmpty} machinery. Value escaping is selected by {@link SlotArray#encoder} and
 *       {@link SlotArray#asciiFastPath}.</li>
 * </ul>
 * The slot-array path is functionally equivalent to the log4j path and produces byte-for-byte identical output; this is
 * asserted in {@link SlotArray#setupSlotArray()} against a reference log4j layout before any measurement runs.
 *
 * <p>Both approaches mirror the real phase split of {@code LoggingAuditTrail}: the message is assembled once - one
 * null-guarded store per present field, as {@code LogEntryBuilder.with(key, value)} does - and the render phase formats that
 * pre-built message. In particular the slot-array's message (which is both the assembled state and the renderer) is allocated
 * during assembly, not at render time, so each phase is charged its real cost.
 *
 * <p>The two approaches live in separate concrete subclasses that share this abstract base. This is deliberate: the
 * {@link SlotArray#encoder}-style knobs only affect the slot-array path, so keeping them off the log4j subclass avoids
 * running the log4j benchmarks redundantly across parameter combinations they are indifferent to. JMH honors
 * {@link State}/{@link Param}/
 * {@link Setup} inheritance and only generates benchmarks for {@code @Benchmark} methods found in concrete subclasses, so the
 * base contributes shared state and setup but no benchmarks of its own.
 *
 * <p>Rather than using the concrete audit field names and values, fields are synthesized with a fixed average name/value length:
 * cost here is driven by the field count and the number of characters walked, not by the specific field identities.
 * {@link #fieldCount} is the number of <em>possible</em> fields (the audit pattern declares an entry for each);
 * {@link #occupancy} is the fraction actually populated per event (absent fields are skipped by {@code %varsNotEmpty} on the
 * log4j side and by a null check on the slot-array side); and {@link #escapeFraction} is the fraction of populated values that
 * contain a {@code "} needing JSON escaping. Both paths also sweep {@link #insertionOrder} - the order fields are added during
 * assembly - because each normalizes that arbitrary order into the same fixed output order but pays for it differently: the
 * log4j {@link StringMapMessage} sorts into a backing array (binary search + shift per {@code put}), while the slot array
 * resolves each field's fixed slot via a {@code name -> slot} map lookup and stores positionally.
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public abstract class AuditLogPatternLayoutBenchmark {

    // Average length of an audit field name, e.g. "authentication.token.type" / "user.run_by.realm_domain".
    private static final int FIELD_NAME_LENGTH = 15;
    // Representative average length of an audit field value (ids, names, realms, actions, addresses, ...).
    private static final int FIELD_VALUE_LENGTH = 24;

    static final String LOGGER_NAME = "org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail";

    @Param({ "32", "64" })
    public int fieldCount;

    @Param({ "0.0", "0.1" })
    public double escapeFraction;

    @Param({ "0.5", "1.0" })
    public double occupancy;

    /**
     * Order in which fields are added during assembly. Both paths normalize this into the same fixed output order, but pay
     * differently: the log4j {@link StringMapMessage} does a binary-search + shift per {@code put} to stay sorted, while the slot
     * array does a {@code name -> slot} map lookup + positional store. {@code SORTED} is ascending key order (best case for the
     * sorted map: appends only); {@code SHUFFLED} models the arbitrary-but-fixed field order of the real audit trail.
     */
    @Param({ "SORTED", "SHUFFLED" })
    public InsertionOrder insertionOrder;

    // Shared assembled state: all possible field names, and the index-aligned values. Absent fields (see occupancy) hold null.
    String[] fieldNames;
    String[] fieldValues;

    // Precomputed insertion sequence (a permutation of field indices), i.e. the order fields are added during assembly.
    int[] order;

    /**
     * Synthesizes the field names/values shared by both approaches. Called explicitly at the start of each subclass's
     * {@code @Setup} rather than being one itself: JMH does not guarantee the execution order of fixture methods at the same
     * {@link org.openjdk.jmh.annotations.Level Level} across a class hierarchy, so a subclass setup cannot assume a base
     * {@code @Setup} has already run.
     *
     * <p>{@code fieldNames} always covers all {@code fieldCount} <em>possible</em> fields (the audit pattern declares an entry
     * for each, regardless of whether a given event populates it). {@link #occupancy} then controls how many are actually
     * <em>present</em>: absent fields hold a {@code null} value, mirroring {@code LogEntryBuilder.with(key, value)} which skips
     * nulls. {@link #escapeFraction} is applied to the present subset.
     */
    protected void initFields() {
        final int present = (int) Math.round(fieldCount * occupancy);
        final int escaping = (int) Math.round(present * escapeFraction);
        // Zero-pad the index suffix to a fixed width so ascending index order equals ascending lexicographic (key) order.
        final int suffixWidth = Integer.toString(fieldCount - 1).length();
        fieldNames = new String[fieldCount];
        fieldValues = new String[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldNames[i] = fieldName(i, suffixWidth);
            fieldValues[i] = i < present ? fieldValue(i, i < escaping) : null;
        }
        order = insertionOrder.sequence(fieldCount);
    }

    /**
     * Order in which assembly adds fields. Both paths render in the same fixed order regardless; this only sets how far the
     * log4j {@link StringMapMessage} must shift to stay sorted (and is the order values arrive in for the slot array's
     * {@code name -> slot} routing). Field names are zero-padded so that ascending index order is ascending key order.
     */
    public enum InsertionOrder {
        /** Ascending key order: every log4j {@code put} appends at the tail of the sorted array - no shifting (best case). */
        SORTED {
            @Override
            int[] sequence(int fieldCount) {
                final int[] order = new int[fieldCount];
                for (int i = 0; i < fieldCount; i++) {
                    order[i] = i;
                }
                return order;
            }
        },
        /**
         * A deterministic (fixed-seed) permutation, modeling the arbitrary-but-fixed field order of the real audit trail: for
         * the log4j path most {@code put}s land ahead of existing keys and shift the sorted arrays.
         */
        SHUFFLED {
            @Override
            int[] sequence(int fieldCount) {
                final int[] order = SORTED.sequence(fieldCount);
                final Random random = new Random(fieldCount); // seed by size for reproducible, size-specific shuffles
                for (int i = fieldCount - 1; i > 0; i--) {
                    final int j = random.nextInt(i + 1);
                    final int tmp = order[i];
                    order[i] = order[j];
                    order[j] = tmp;
                }
                return order;
            }
        };

        abstract int[] sequence(int fieldCount);
    }

    /**
     * The production audit path: a {@link StringMapMessage} assembled field-by-field and rendered through the
     * {@code %varsNotEmpty{...%enc{%map{...}}{JSON}...}} pattern layout. Escaping is fixed by the pattern's {@code %enc}
     * converter, so this subclass declares none of the slot-array's escaping knobs.
     */
    public static class Log4j extends AuditLogPatternLayoutBenchmark {

        private PatternLayout layout;
        // Reused per-thread event carrier, so assembleAndRender is charged for message assembly + render, not per-event
        // LogEvent allocation - matching the slot-array path (and Log4j's own GC-free mode).
        private final MutableLogEvent event = new MutableLogEvent();

        @Setup
        public void setupLog4j() {
            initFields();
            final List<String> names = new ArrayList<>(fieldCount);
            Collections.addAll(names, fieldNames);
            layout = PatternLayout.newBuilder().setPattern(buildPattern(names)).setCharset(StandardCharsets.UTF_8).build();
            event.setLoggerName(LOGGER_NAME);
            event.setLevel(Level.INFO);
            event.setTimeMillis(System.currentTimeMillis());
            event.setMessage(assemble());
        }

        /** Assembly phase: build the {@link StringMapMessage} field-by-field in {@link #insertionOrder}, as {@code LogEntryBuilder} does. */
        @Benchmark
        public StringMapMessage assemble() {
            return newMapMessage(fieldNames, fieldValues, order);
        }

        /** Render phase: format a pre-assembled event into a JSON line via the audit pattern layout. */
        @Benchmark
        public String render() {
            return layout.toSerializable(event);
        }

        /** Combined assembly + render, i.e. the full per-event cost. */
        @Benchmark
        public String assembleAndRender() {
            event.setMessage(assemble());
            return layout.toSerializable(event);
        }
    }

    /**
     * The slot-array path: field values stored in a {@code String[]} slot array and rendered through a
     * {@code %m%n} layout via {@link SlotArrayMessage#formatTo}. {@link #encoder} and {@link #asciiFastPath} are the two
     * orthogonal escaping knobs exercised here; they do not exist on {@link Log4j}, so its benchmarks are not re-run for them.
     */
    public static class SlotArray extends AuditLogPatternLayoutBenchmark {

        @Param({ "XCONTENT", "LOG4J" })
        public Encoder encoder;

        @Param({ "true", "false" })
        public boolean asciiFastPath;

        private PatternLayout layout;
        // Reused per-thread event carrier for the %m%n layout, avoiding a per-event allocation (as Log4j does in GC-free mode).
        private final MutableLogEvent event = new MutableLogEvent();

        // Fixed name -> slot mapping, resolving a field name to its position in the slot array. This is the per-field lookup the
        // real assembly pays (it accepts values keyed by name, not by position) and what replaces the sorted map's insert cost.
        private Map<String, Integer> slotByName;

        @Setup
        public void setupSlotArray() {
            initFields();
            slotByName = new HashMap<>((int) (fieldCount / 0.75f) + 1);
            for (int i = 0; i < fieldCount; i++) {
                slotByName.put(fieldNames[i], i);
            }
            layout = PatternLayout.newBuilder().setPattern("%m%n").setCharset(StandardCharsets.UTF_8).build();
            event.setLoggerName(LOGGER_NAME);
            event.setLevel(Level.INFO);

            // Reference log4j layout/event, built here only to validate byte-for-byte equivalence. It is never benchmarked.
            final List<String> names = new ArrayList<>(fieldCount);
            Collections.addAll(names, fieldNames);
            final PatternLayout referenceLayout = PatternLayout.newBuilder()
                .setPattern(buildPattern(names))
                .setCharset(StandardCharsets.UTF_8)
                .build();
            final LogEvent referenceEvent = newMapEvent(newMapMessage(fieldNames, fieldValues));

            // The slot-array message is assembled once and reused by render(), mirroring the log4j path which formats a
            // message built up front. This keeps the message allocation attributed to the assembly phase, not to render.
            event.setMessage(assemble());

            final String expected = referenceLayout.toSerializable(referenceEvent);
            final String actual = layout.toSerializable(event);
            if (expected.equals(actual) == false) {
                throw new AssertionError("slot-array output differs from log4j output:\nexpected=" + expected + "\nactual  =" + actual);
            }
        }

        /**
         * Assembly phase: for each present field, in {@link #insertionOrder}, resolve its fixed slot via the {@code name -> slot}
         * map and store the value there - mirroring {@code LogEntryBuilder.with(key, value)}, which is handed values keyed by
         * name (not position) and so must map name to slot. The filled slots are wrapped in the {@link SlotArrayMessage} that is
         * both the assembled state and the renderer. Absent fields (null) are skipped. Rendering is always in fixed slot order, so
         * the insertion order changes only the sequence of lookups here, not the output.
         */
        @Benchmark
        public Message assemble() {
            final String[] slots = new String[fieldCount];
            for (int idx : order) {
                final String value = fieldValues[idx];
                if (value != null) {
                    slots[slotByName.get(fieldNames[idx])] = value;
                }
            }
            return new SlotArrayMessage(fieldNames, slots, encoder, asciiFastPath);
        }

        /**
         * Render phase: emit the pre-assembled message through the {@code %m%n} layout. Because {@link SlotArrayMessage}
         * is {@link StringBuilderFormattable}, the {@code %m} converter calls {@code formatTo} directly on Log4j's buffer, so the
         * JSON is built in a single pass with no intermediate string.
         */
        @Benchmark
        public String render() {
            return layout.toSerializable(event);
        }

        /** Combined assembly + render, i.e. the full per-event cost. */
        @Benchmark
        public String assembleAndRender() {
            event.setMessage(assemble());
            return layout.toSerializable(event);
        }

        /**
         * JSON value escaping strategy for the slot-array path. Each strategy always escapes; the optional
         * {@link #asciiFastPath} guard (applied before this is called) may skip it entirely for values known not to require
         * escaping.
         */
        public enum Encoder {
            /**
             * XContent's {@link JsonStringEncoder} (Jackson): {@code quoteAsString} copies the value char-by-char into the
             * buffer, allocating a small scratch buffer per escape.
             */
            XCONTENT {
                @Override
                void appendEscaped(StringBuilder buffer, String value) {
                    JsonStringEncoder.getInstance().quoteAsString(value, buffer);
                }
            },
            /**
             * Log4j's {@link StringBuilders#escapeJson} - the same escaper the {@code %enc{...}{JSON}} converter uses. The value
             * is appended verbatim and then escaped in place, with no per-value allocation and no copy when nothing needs
             * escaping.
             */
            LOG4J {
                @Override
                void appendEscaped(StringBuilder buffer, String value) {
                    final int start = buffer.length();
                    buffer.append(value);
                    StringBuilders.escapeJson(buffer, start);
                }
            };

            abstract void appendEscaped(StringBuilder buffer, String value);
        }

        /**
         * A value is ASCII-safe when JSON encoding would not change it: every character is printable ASCII and is neither a
         * quote nor a backslash. Such values can be appended verbatim, skipping the encoder's scan and (for XContent) its
         * output buffer.
         */
        private static boolean isAsciiSafe(String value) {
            for (int i = 0; i < value.length(); i++) {
                final char c = value.charAt(i);
                if (c < 0x20 || c > 0x7E || c == '"' || c == '\\') {
                    return false;
                }
            }
            return true;
        }

        /**
         * Audit log message for the slot-array path: it is both the assembled state (an ordered slot array) and the renderer.
         * As a {@link StringBuilderFormattable}, {@code %m} formats it straight into Log4j's buffer.
         *
         * <p>The {@link AsynchronouslyFormattable} annotation is essential for a fair comparison. When
         * {@code MutableLogEvent.setMessage} is handed a message that is <em>not</em> asynchronously-formattable, it eagerly calls
         * {@code getFormattedMessage()} on it (via {@code InternalAsyncUtil.makeMessageImmutable}, LOG4J2-763) to snapshot it -
         * so the message is rendered <b>twice</b> per event: once here, and again by the {@code %m} converter. That doubles both
         * time and allocation and erases the slot-array's advantage. {@link StringMapMessage} carries this annotation, so the
         * log4j path is exempt; the real audit accumulator carries it too. Without it the two paths would not be compared on
         * equal footing.
         */
        @AsynchronouslyFormattable
        private static final class SlotArrayMessage implements Message, StringBuilderFormattable {

            private static final long serialVersionUID = 1L;

            private final String[] names;
            private final String[] values;
            private final Encoder encoder;
            private final boolean asciiFastPath;

            SlotArrayMessage(String[] names, String[] values, Encoder encoder, boolean asciiFastPath) {
                this.names = names;
                this.values = values;
                this.encoder = encoder;
                this.asciiFastPath = asciiFastPath;
            }

            @Override
            public void formatTo(StringBuilder buffer) {
                buffer.append("{\"type\":\"audit\"");
                for (int i = 0; i < values.length; i++) {
                    final String value = values[i];
                    if (value != null && value.isEmpty() == false) {
                        buffer.append(", \"").append(names[i]).append("\":\"");
                        if (asciiFastPath && isAsciiSafe(value)) {
                            buffer.append(value);
                        } else {
                            encoder.appendEscaped(buffer, value);
                        }
                        buffer.append('"');
                    }
                }
                buffer.append('}');
            }

            @Override
            public String getFormattedMessage() {
                final StringBuilder buffer = new StringBuilder(128);
                formatTo(buffer);
                return buffer.toString();
            }

            @Override
            public String getFormat() {
                return "";
            }

            @Override
            public Object[] getParameters() {
                return null;
            }

            @Override
            public Throwable getThrowable() {
                return null;
            }
        }
    }

    /**
     * Builds a {@link StringMapMessage} from the index-aligned name/value arrays, adding present fields in the given insertion
     * order - field-by-field as {@code LogEntryBuilder} does, including its {@code if (value != null)} guard so absent fields are
     * simply not added. The insertion order is what determines how many elements Log4j's {@code SortedArrayStringMap} must shift
     * on each {@code put} to keep its backing arrays sorted; it does not affect the rendered order (the pattern emits fields in
     * pattern order via {@code %map} key lookups).
     */
    static StringMapMessage newMapMessage(String[] names, String[] values, int[] order) {
        final StringMapMessage message = new StringMapMessage(names.length);
        for (int idx : order) {
            if (values[idx] != null) {
                message.with(names[idx], values[idx]);
            }
        }
        return message;
    }

    /** Builds a {@link StringMapMessage} adding present fields in natural (ascending index) order. */
    static StringMapMessage newMapMessage(String[] names, String[] values) {
        final int[] order = new int[names.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        return newMapMessage(names, values, order);
    }

    static LogEvent newMapEvent(StringMapMessage message) {
        return Log4jLogEvent.newBuilder()
            .setLoggerName(LOGGER_NAME)
            .setLevel(Level.INFO)
            .setMessage(message)
            .setTimeMillis(System.currentTimeMillis())
            .build();
    }

    /**
     * Generates the audit-appender pattern, one entry per field, e.g.
     * {@code %varsNotEmpty{, "field.fffffff12":"%enc{%map{field.fffffff12}}{JSON}"}}
     */
    private static String buildPattern(List<String> names) {
        final StringBuilder pattern = new StringBuilder();
        pattern.append("{\"type\":\"audit\"");
        for (String name : names) {
            pattern.append("%varsNotEmpty{, \"").append(name).append("\":\"%enc{%map{").append(name).append("}}{JSON}\"}");
        }
        pattern.append("}%n");
        return pattern.toString();
    }

    // A field name of FIELD_NAME_LENGTH chars, made unique by embedding the zero-padded index. Zero-padding to a fixed width
    // keeps ascending index order equal to ascending lexicographic order, so InsertionOrder.SORTED maps onto the order in which
    // StringMapMessage's backing SortedArrayStringMap wants its keys (appends only, no shifting).
    private static String fieldName(int index, int suffixWidth) {
        final String suffix = String.format(Locale.ROOT, "%0" + suffixWidth + "d", index);
        return "field." + "f".repeat(Math.max(1, FIELD_NAME_LENGTH - "field.".length() - suffixWidth)) + suffix;
    }

    // A value of FIELD_VALUE_LENGTH chars, made unique by embedding the index. When escape is set, a '"' is embedded so the
    // value is not ASCII-safe and must go through JSON escaping (which turns it into \").
    private static String fieldValue(int index, boolean escape) {
        final String suffix = Integer.toString(index);
        final int fill = Math.max(1, FIELD_VALUE_LENGTH - suffix.length() - (escape ? 1 : 0));
        final StringBuilder value = new StringBuilder(FIELD_VALUE_LENGTH);
        value.append("v".repeat(fill));
        if (escape) {
            value.append('"');
        }
        return value.append(suffix).toString();
    }
}
