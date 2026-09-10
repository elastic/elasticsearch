/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.api.internal.ImmutableSpanContext;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.time.Instant;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.TelemetryLogEventFilter;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Log4j {@link AbstractAppender} that bridges log events to the OTel logs API,
 * used for querylog and audit log delivery via OTLP.
 *
 * <p>Improvements over the upstream {@code OpenTelemetryAppender}:
 * <ul>
 *   <li>No {@code log4j.map_message.} prefix on MapMessage keys — attributes are emitted verbatim.</li>
 *   <li>Typed attribute values: Long/Integer → longKey, Double/Float → doubleKey,
 *       Boolean → booleanKey, List → typed array attribute, Map → valueKey with {@link Value}.</li>
 * </ul>
 */
public class ElasticsearchOtelAppender extends AbstractAppender {

    private static final String MESSAGE_KEY = "message";
    private static final String TRACE_ID_KEY = "trace.id";

    private volatile OpenTelemetry openTelemetry;
    /**
     * Optional message filter which can modify or discard log events.
     */
    @Nullable
    private final TelemetryLogEventFilter filter;

    private static final int KEY_CACHE_MAX_SIZE = 100;

    // Per-type attribute key caches, bounded to avoid unbounded growth on high-cardinality key sets.
    private static final Map<String, AttributeKey<String>> STRING_KEYS = boundedCache();
    private static final Map<String, AttributeKey<Long>> LONG_KEYS = boundedCache();
    private static final Map<String, AttributeKey<Double>> DOUBLE_KEYS = boundedCache();
    private static final Map<String, AttributeKey<Boolean>> BOOLEAN_KEYS = boundedCache();
    private static final Map<String, AttributeKey<List<String>>> STRING_ARRAY_KEYS = boundedCache();
    private static final Map<String, AttributeKey<List<Long>>> LONG_ARRAY_KEYS = boundedCache();
    private static final Map<String, AttributeKey<List<Double>>> DOUBLE_ARRAY_KEYS = boundedCache();
    private static final Map<String, AttributeKey<List<Boolean>>> BOOLEAN_ARRAY_KEYS = boundedCache();
    private static final Map<String, AttributeKey<Value<?>>> VALUE_KEYS = boundedCache();

    private static <K, V> Map<K, V> boundedCache() {
        return Collections.synchronizedMap(new LinkedHashMap<>(KEY_CACHE_MAX_SIZE) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > KEY_CACHE_MAX_SIZE;
            }
        });
    }

    /**
     * @param name          appender name used by the log4j configuration graph
     * @param openTelemetry initial OTel instance; may be updated atomically via {@link #setOpenTelemetry}
     * @param filter        optional filter applied before emitting each event; {@code null} means no filtering
     */
    public ElasticsearchOtelAppender(String name, OpenTelemetry openTelemetry, @Nullable TelemetryLogEventFilter filter) {
        super(name, null, null, true, Property.EMPTY_ARRAY);
        Objects.requireNonNull(openTelemetry, "openTelemetry is null");
        this.openTelemetry = openTelemetry;
        this.filter = filter;
    }

    /**
     * Hot-swap the OTel instance. Safe to call concurrently with {@link #append}.
     */
    public void setOpenTelemetry(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "openTelemetry is null");
        this.openTelemetry = openTelemetry;
    }

    @Override
    public void append(LogEvent event) {
        emit(openTelemetry, event);
    }

    private void emit(OpenTelemetry ot, LogEvent event) {
        String instrumentationName = event.getLoggerName();
        if (instrumentationName == null || instrumentationName.isEmpty()) {
            instrumentationName = "ROOT";
        }
        LogRecordBuilder builder = ot.getLogsBridge().loggerBuilder(instrumentationName).build().logRecordBuilder();

        Level level = event.getLevel();
        if (level != null) {
            builder.setSeverity(levelToSeverity(level));
            builder.setSeverityText(level.name());
        }

        Throwable throwable = event.getThrown();
        if (throwable != null) {
            builder.setException(throwable);
        }

        Message message = event.getMessage();
        Context ctx = Context.current();
        if (message instanceof MapMessage<?, ?> mapMessage) {
            if (ctx == Context.root()) {
                ctx = traceContextFromMapMessage(mapMessage, ctx);
            }
            if (captureMapMessage(builder, mapMessage) == false) {
                return;
            }
        } else if (message != null) {
            builder.setBody(message.getFormattedMessage());
        }
        builder.setContext(ctx);

        Instant ts = event.getInstant();
        if (ts != null) {
            builder.setTimestamp(TimeUnit.MILLISECONDS.toNanos(ts.getEpochMillisecond()) + ts.getNanoOfMillisecond(), TimeUnit.NANOSECONDS);
        }

        builder.emit();
    }

    private static Context traceContextFromMapMessage(MapMessage<?, ?> mapMessage, Context fallback) {
        String traceId = mapMessage.get(TRACE_ID_KEY);
        if (traceId != null && traceId.isEmpty() == false) {
            SpanContext spanCtx = ImmutableSpanContext.create(
                traceId,
                SpanId.getInvalid(),
                TraceFlags.getSampled(),
                TraceState.getDefault(),
                true,
                true
            );
            return fallback.with(Span.wrap(spanCtx));
        }
        return fallback;
    }

    @SuppressWarnings("unchecked")
    private boolean captureMapMessage(LogRecordBuilder builder, MapMessage<?, ?> mapMessage) {
        String body = mapMessage.getFormat();
        boolean useMessageKey = (body == null || body.isEmpty());
        if (useMessageKey) {
            body = mapMessage.get(MESSAGE_KEY);
        }
        if (body != null && body.isEmpty() == false) {
            builder.setBody(body);
        }

        Map<String, Object> data = (Map<String, Object>) mapMessage.getData();
        if (filter != null) {
            data = filter.filter(data);
            if (data == null) return false;
        }

        data.forEach((key, value) -> {
            if (value != null && shouldEmitAsAttribute(key, useMessageKey)) {
                setTypedAttribute(builder, key, value);
            }
        });
        return true;
    }

    private static boolean shouldEmitAsAttribute(String key, boolean useMessageKey) {
        return TRACE_ID_KEY.equals(key) == false && (useMessageKey == false || MESSAGE_KEY.equals(key) == false);
    }

    static <T> List<T> arrayToList(Object array, Function<Object, T> mapper) {
        return IntStream.range(0, Array.getLength(array)).mapToObj(i -> mapper.apply(Array.get(array, i))).toList();
    }

    private static void setTypedAttribute(LogRecordBuilder builder, String key, Object value) {
        switch (value) {
            case String s -> builder.setAttribute(STRING_KEYS.computeIfAbsent(key, AttributeKey::stringKey), s);
            case Boolean b -> builder.setAttribute(BOOLEAN_KEYS.computeIfAbsent(key, AttributeKey::booleanKey), b);
            case Long l -> builder.setAttribute(LONG_KEYS.computeIfAbsent(key, AttributeKey::longKey), l);
            case Integer i -> builder.setAttribute(LONG_KEYS.computeIfAbsent(key, AttributeKey::longKey), (long) i);
            case Double d -> builder.setAttribute(DOUBLE_KEYS.computeIfAbsent(key, AttributeKey::doubleKey), d);
            case Float f -> builder.setAttribute(DOUBLE_KEYS.computeIfAbsent(key, AttributeKey::doubleKey), (double) f);
            case List<?> list -> setListAttribute(builder, key, list);
            case boolean[] arr -> builder.setAttribute(
                BOOLEAN_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::booleanArrayKey),
                arrayToList(arr, o -> (Boolean) o)
            );
            case long[] arr -> builder.setAttribute(
                LONG_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::longArrayKey),
                Arrays.stream(arr).boxed().toList()
            );
            case int[] arr -> builder.setAttribute(
                LONG_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::longArrayKey),
                Arrays.stream(arr).asLongStream().boxed().toList()
            );
            case double[] arr -> builder.setAttribute(
                DOUBLE_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::doubleArrayKey),
                Arrays.stream(arr).boxed().toList()
            );
            case float[] arr -> builder.setAttribute(
                DOUBLE_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::doubleArrayKey),
                arrayToList(arr, o -> Double.valueOf((float) o))
            );
            case Object[] arr -> setListAttribute(builder, key, Arrays.asList(arr));
            case Map<?, ?> map -> {
                if (map.isEmpty() == false) {
                    builder.setAttribute(VALUE_KEYS.computeIfAbsent(key, AttributeKey::valueKey), Value.of(toValueMap(map)));
                }
            }
            default -> builder.setAttribute(STRING_KEYS.computeIfAbsent(key, AttributeKey::stringKey), value.toString());
        }
    }

    /**
     * Probes the first non-null element to pick an OTel typed-array key.
     * Homogeneity is not verified — the caller is trusted; OTel will reject at export if it can't support it.
     */
    private static void setListAttribute(LogRecordBuilder builder, String key, List<?> list) {
        if (list.isEmpty()) {
            return;
        }
        switch (list.getFirst()) {
            case String ignored -> builder.setAttribute(
                STRING_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::stringArrayKey),
                castList(list)
            );
            case Boolean ignored -> builder.setAttribute(
                BOOLEAN_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::booleanArrayKey),
                castList(list)
            );
            case Long ignored -> builder.setAttribute(LONG_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::longArrayKey), castList(list));
            case Integer ignored -> builder.setAttribute(
                LONG_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::longArrayKey),
                list.stream().mapToLong(o -> (Integer) o).boxed().toList()
            );
            case Double ignored -> builder.setAttribute(
                DOUBLE_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::doubleArrayKey),
                castList(list)
            );
            case Float ignored -> builder.setAttribute(
                DOUBLE_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::doubleArrayKey),
                list.stream().map(o -> (double) (Float) o).toList()
            );
            case Map<?, ?> ignored -> builder.setAttribute(
                VALUE_KEYS.computeIfAbsent(key, AttributeKey::valueKey),
                Value.of(list.stream().map(ElasticsearchOtelAppender::toValue).toArray(Value[]::new))
            );
            default -> builder.setAttribute(
                STRING_ARRAY_KEYS.computeIfAbsent(key, AttributeKey::stringArrayKey),
                list.stream().map(o -> o == null ? "" : o.toString()).toList()
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> castList(List<?> list) {
        return (List<T>) list;
    }

    static Value<?> toValue(Object o) {
        return switch (o) {
            case String s -> Value.of(s);
            case Boolean b -> Value.of(b);
            case Long l -> Value.of(l);
            case Integer i -> Value.of((long) i);
            case Double d -> Value.of(d);
            case Float f -> Value.of((double) f);
            case Map<?, ?> m -> Value.of(toValueMap(m));
            case List<?> l -> Value.of(l.stream().map(ElasticsearchOtelAppender::toValue).toArray(Value[]::new));
            case Object[] arr -> Value.of(Arrays.stream(arr).map(ElasticsearchOtelAppender::toValue).toArray(Value[]::new));
            default -> Value.of(o.toString());
        };
    }

    private static Map<String, Value<?>> toValueMap(Map<?, ?> map) {
        Map<String, Value<?>> result = new LinkedHashMap<>(map.size());
        map.forEach((k, v) -> {
            if (v != null) {
                result.put(String.valueOf(k), toValue(v));
            }
        });
        return result;
    }

    private static Severity levelToSeverity(Level level) {
        return switch (level.getStandardLevel()) {
            case TRACE -> Severity.TRACE;
            case DEBUG -> Severity.DEBUG;
            case INFO -> Severity.INFO;
            case WARN -> Severity.WARN;
            case ERROR -> Severity.ERROR;
            case FATAL -> Severity.FATAL;
            case OFF, ALL -> Severity.UNDEFINED_SEVERITY_NUMBER;
        };
    }
}
