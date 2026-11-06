/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Read profiling for doc values codecs. A codec records counts and call times of its readers through this module. A
 * frontend (the search profile or the ES|QL profile) collects and reports them. The module knows no codec and no
 * frontend.
 *
 * <h2>Main abstractions</h2>
 * <ul>
 *   <li>{@link org.elasticsearch.codec.profiler.MetricRecorder}: the codec-facing interface. It hands out handles.</li>
 *   <li>{@link org.elasticsearch.codec.profiler.Counter} and {@link org.elasticsearch.codec.profiler.Timer}: the
 *   handles. A counter adds non-negative values. A timer brackets calls with {@code start} and {@code stop}.</li>
 *   <li>{@link org.elasticsearch.codec.profiler.CounterMetric} and {@link org.elasticsearch.codec.profiler.TimerMetric}:
 *   metric definitions. A definition has an identity ({@link org.elasticsearch.codec.profiler.Metric.Id}: codec,
 *   component, name), a unit and a description. A timer definition also has a
 *   {@link org.elasticsearch.codec.profiler.TimingPolicy} and an optional CPU time companion.</li>
 *   <li>{@link org.elasticsearch.codec.profiler.Scope} and {@link org.elasticsearch.codec.profiler.Scopes}: the
 *   frontend binds a scope on a thread, and a codec gets its recorder from the bound scope.</li>
 *   <li>{@link org.elasticsearch.codec.profiler.ProfileCollector}: the recorder that a frontend creates and owns. A
 *   collector can have detail collectors. Every event on a detail also reaches its parent.</li>
 *   <li>{@link org.elasticsearch.codec.profiler.Snapshot}: the immutable result of a collector. It holds one
 *   measurement per metric, and each measurement carries its definition.</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>The frontend binds a scope with {@link org.elasticsearch.codec.profiler.Scopes#bind} around the code that opens
 *   readers. The scope returns a collector, or a detail collector, for each reader. It can create the collectors
 *   before the binding or when the first reader asks.</li>
 *   <li>The codec calls {@link org.elasticsearch.codec.profiler.Scopes#current} once when it opens a reader. It keeps
 *   the returned recorder for the life of the reader. The result is null when no scope is bound.</li>
 *   <li>When the codec builds a component that records, it asks the recorder for its counters and timers. Asking
 *   registers the metric.</li>
 *   <li>On the read path the codec calls {@code add}, {@code start} and {@code stop}. Work done after the binding is
 *   closed, for example during iteration, still goes to the collector of the reader.</li>
 *   <li>The frontend takes snapshots with {@link org.elasticsearch.codec.profiler.ProfileCollector#snapshot()} and
 *   sums the snapshots of disjoint work with {@link org.elasticsearch.codec.profiler.Snapshot#merge}.</li>
 * </ol>
 *
 * <h2>Measurement semantics</h2>
 * <ul>
 *   <li>Counters are exact. Every count and time saturates at {@code Long.MAX_VALUE}.</li>
 *   <li>A timer counts every call. Its policy decides which calls it measures. A timer with unmeasured calls reports
 *   estimated time. {@link org.elasticsearch.codec.profiler.TimingPolicy} and
 *   {@link org.elasticsearch.codec.profiler.Timer} give the details.</li>
 *   <li>Timer values are elapsed time in nanoseconds. A timer with a CPU time companion also reports the thread CPU
 *   time of the same measured calls when its collector measures CPU time.</li>
 *   <li>The measuring decisions of a parent collector are the same with or without details. When every reader records
 *   into a detail, the counters, the call counts and the {@code EXACT} timers of the details sum to the values of the
 *   parent. {@code ADAPTIVE} timers do not sum exactly, because each detail estimates its own calls.</li>
 *   <li>A metric is absent from a snapshot when no component registered it. Absent is not zero. Times of nested
 *   operations overlap, so do not add or subtract them.</li>
 * </ul>
 *
 * <h2>Design constraints</h2>
 * <ul>
 *   <li>The module is codec independent. Each codec owns its metric definitions and declares them as constants next to
 *   the code that records them. A metric identity has one unit and one meaning: a collector refuses a second
 *   definition of an identity, and so does a merge.</li>
 *   <li>Instrumentation is optional. A reader opened with no scope bound has no recorder. The codec then uses
 *   {@link org.elasticsearch.codec.profiler.Timer#INERT} and records no counts. That read path reads no clock and
 *   allocates nothing for profiling. The only cost is one thread-local lookup when the reader opens.</li>
 *   <li>A collector, its details and their handles have one writer at a time. A snapshot needs the same exclusive
 *   access, or a completed handoff. Calls of one timer must not overlap. Assertions check these preconditions.</li>
 *   <li>A recorder and the handles taken from it belong to the execution whose scope was bound when the reader opened.
 *   Only an object whose instrumented work all belongs to that execution may keep them, for example a reader that one
 *   execution opens and no other execution uses. A component that serves several executions, such as a cache in the
 *   doc values producer, never keeps them: it records with the handles of the caller it works for. A reader cache
 *   that hands one reader to several executions breaks this rule, and so does a merge that takes the scope of an
 *   unrelated execution bound on its thread. Work that no execution-owned object performs, for example in a shared
 *   background component, cannot be attributed to an execution by this module.</li>
 *   <li>The frontend decides what to profile, binds scopes, owns the collectors, takes the snapshots and attributes
 *   them to shards, segments and fields. The codec declares metrics, gets its recorder and records. Each side sees the
 *   other only through this module.</li>
 *   <li>Profiling changes what it measures. Clock reads and bookkeeping take time and touch memory. A detail pays
 *   every event twice, and it measures most calls of a reader with few calls. Very short calls carry the largest
 *   relative overhead.</li>
 * </ul>
 *
 * <h2>Adding profiling to a codec</h2>
 * <ol>
 *   <li>Declare {@link org.elasticsearch.codec.profiler.CounterMetric} and
 *   {@link org.elasticsearch.codec.profiler.TimerMetric} constants with the codec name as group.
 *   {@link org.elasticsearch.codec.profiler.Metric} gives the naming rules.</li>
 *   <li>When a reader opens, call {@link org.elasticsearch.codec.profiler.Scopes#current} with the name of the doc
 *   values format, the segment, the field and the facts about the stored column.</li>
 *   <li>At construction, get the handles from the recorder. Use {@code Timer.INERT} when the recorder is null.</li>
 *   <li>Bracket each timed call as the {@link org.elasticsearch.codec.profiler.Timer} example shows.</li>
 *   <li>Document the metrics beside the codec. The reference for ColumNAR is the Javadoc of
 *   {@code org.elasticsearch.columnar}.</li>
 * </ol>
 */
module org.elasticsearch.codec.profiler {
    requires org.elasticsearch.base;
    requires java.management;

    exports org.elasticsearch.codec.profiler;
}
