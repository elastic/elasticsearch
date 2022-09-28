/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.script.DynamicMap;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

public class ConditionalProcessor extends AbstractProcessor implements WrappingProcessor {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> FUNCTIONS = org.elasticsearch.core.Map.of("_type", value -> {
        deprecationLogger.critical(
            DeprecationCategory.SCRIPTING,
            "conditional-processor__type",
            "[types removal] Looking up doc types [_type] in scripts is deprecated."
        );
        return value;
    });

    private static final Logger logger = LogManager.getLogger(ConditionalProcessor.class);

    static final String TYPE = "conditional";

    private final Script condition;
    private final ScriptService scriptService;
    private final Processor processor;
    private final IngestMetric metric;
    private final LongSupplier relativeTimeProvider;
    private final IngestConditionalScript precompiledConditionScript;

    ConditionalProcessor(String tag, String description, Script script, ScriptService scriptService, Processor processor) {
        this(tag, description, script, scriptService, processor, System::nanoTime);
    }

    ConditionalProcessor(
        String tag,
        String description,
        Script script,
        ScriptService scriptService,
        Processor processor,
        LongSupplier relativeTimeProvider
    ) {
        super(tag, description);
        this.condition = script;
        this.scriptService = scriptService;
        this.processor = processor;
        this.metric = new IngestMetric();
        this.relativeTimeProvider = relativeTimeProvider;

        try {
            final IngestConditionalScript.Factory factory = scriptService.compile(script, IngestConditionalScript.CONTEXT);
            if (ScriptType.INLINE.equals(script.getType())) {
                precompiledConditionScript = factory.newInstance(script.getParams());
            } else {
                // stored script, so will have to compile at runtime
                precompiledConditionScript = null;
            }
        } catch (ScriptException e) {
            throw newConfigurationException(TYPE, tag, null, e);
        }
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        final boolean matches;
        try {
            matches = evaluate(ingestDocument);
        } catch (Exception e) {
            handler.accept(null, e);
            return;
        }

        if (matches) {
            final long startTimeInNanos = relativeTimeProvider.getAsLong();
            /*
             * Our assumption is that the listener passed to the processor is only ever called once. However, there is no way to enforce
             * that in all processors and all of the code that they call. If the listener is called more than once it causes problems
             * such as the metrics being wrong. The listenerHasBeenCalled variable is used to make sure that the code in the listener
             * is only executed once.
             */
            final AtomicBoolean listenerHasBeenCalled = new AtomicBoolean(false);
            metric.preIngest();
            processor.execute(ingestDocument, (result, e) -> {
                if (listenerHasBeenCalled.getAndSet(true)) {
                    logger.warn("A listener was unexpectedly called more than once", new RuntimeException());
                    assert false : "A listener was unexpectedly called more than once";
                } else {
                    long ingestTimeInNanos = relativeTimeProvider.getAsLong() - startTimeInNanos;
                    metric.postIngest(ingestTimeInNanos);
                    if (e != null) {
                        metric.ingestFailed();
                        handler.accept(null, e);
                    } else {
                        handler.accept(result, null);
                    }
                }
            });
        } else {
            handler.accept(ingestDocument, null);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    boolean evaluate(IngestDocument ingestDocument) {
        IngestConditionalScript script = precompiledConditionScript;
        if (script == null) {
            IngestConditionalScript.Factory factory = scriptService.compile(condition, IngestConditionalScript.CONTEXT);
            script = factory.newInstance(condition.getParams());
        }
        return script.execute(new UnmodifiableIngestData(new DynamicMap(ingestDocument.getSourceAndMetadata(), FUNCTIONS)));
    }

    public Processor getInnerProcessor() {
        return processor;
    }

    IngestMetric getMetric() {
        return metric;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getCondition() {
        return condition.getIdOrCode();
    }

    @SuppressWarnings("unchecked")
    private static Object wrapUnmodifiable(Object raw) {
        // Wraps all mutable types that the JSON parser can create by immutable wrappers.
        // Any inputs not wrapped are assumed to be immutable
        if (raw instanceof Map) {
            return new UnmodifiableIngestData((Map<String, Object>) raw);
        } else if (raw instanceof List) {
            return new UnmodifiableIngestList((List<Object>) raw);
        } else if (raw instanceof byte[]) {
            return ((byte[]) raw).clone();
        }
        return raw;
    }

    private static UnsupportedOperationException unmodifiableException() {
        return new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
    }

    private static final class UnmodifiableIngestData implements Map<String, Object> {

        private final Map<String, Object> data;

        UnmodifiableIngestData(Map<String, Object> data) {
            this.data = data;
        }

        @Override
        public int size() {
            return data.size();
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public boolean containsKey(final Object key) {
            return data.containsKey(key);
        }

        @Override
        public boolean containsValue(final Object value) {
            return data.containsValue(value);
        }

        @Override
        public Object get(final Object key) {
            return wrapUnmodifiable(data.get(key));
        }

        @Override
        public Object put(final String key, final Object value) {
            throw unmodifiableException();
        }

        @Override
        public Object remove(final Object key) {
            throw unmodifiableException();
        }

        @Override
        public void putAll(final Map<? extends String, ?> m) {
            throw unmodifiableException();
        }

        @Override
        public void clear() {
            throw unmodifiableException();
        }

        @Override
        public Set<String> keySet() {
            return Collections.unmodifiableSet(data.keySet());
        }

        @Override
        public Collection<Object> values() {
            return new UnmodifiableIngestList(new ArrayList<>(data.values()));
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return data.entrySet().stream().map(entry -> new Entry<String, Object>() {
                @Override
                public String getKey() {
                    return entry.getKey();
                }

                @Override
                public Object getValue() {
                    return wrapUnmodifiable(entry.getValue());
                }

                @Override
                public Object setValue(final Object value) {
                    throw unmodifiableException();
                }

                @Override
                public boolean equals(final Object o) {
                    return entry.equals(o);
                }

                @Override
                public int hashCode() {
                    return entry.hashCode();
                }
            }).collect(Collectors.toSet());
        }
    }

    private static final class UnmodifiableIngestList implements List<Object> {

        private final List<Object> data;

        UnmodifiableIngestList(List<Object> data) {
            this.data = data;
        }

        @Override
        public int size() {
            return data.size();
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return data.contains(o);
        }

        @Override
        public Iterator<Object> iterator() {
            Iterator<Object> wrapped = data.iterator();
            return new Iterator<Object>() {
                @Override
                public boolean hasNext() {
                    return wrapped.hasNext();
                }

                @Override
                public Object next() {
                    return wrapped.next();
                }

                @Override
                public void remove() {
                    throw unmodifiableException();
                }
            };
        }

        @Override
        public Object[] toArray() {
            Object[] wrapped = data.toArray(new Object[0]);
            for (int i = 0; i < wrapped.length; i++) {
                wrapped[i] = wrapUnmodifiable(wrapped[i]);
            }
            return wrapped;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(final T[] a) {
            Object[] raw = data.toArray(new Object[0]);
            T[] wrapped = (T[]) Arrays.copyOf(raw, a.length, a.getClass());
            for (int i = 0; i < wrapped.length; i++) {
                wrapped[i] = (T) wrapUnmodifiable(wrapped[i]);
            }
            return wrapped;
        }

        @Override
        public boolean add(final Object o) {
            throw unmodifiableException();
        }

        @Override
        public boolean remove(final Object o) {
            throw unmodifiableException();
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return data.contains(c);
        }

        @Override
        public boolean addAll(final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean addAll(final int index, final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean removeAll(final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public boolean retainAll(final Collection<?> c) {
            throw unmodifiableException();
        }

        @Override
        public void clear() {
            throw unmodifiableException();
        }

        @Override
        public Object get(final int index) {
            return wrapUnmodifiable(data.get(index));
        }

        @Override
        public Object set(final int index, final Object element) {
            throw unmodifiableException();
        }

        @Override
        public void add(final int index, final Object element) {
            throw unmodifiableException();
        }

        @Override
        public Object remove(final int index) {
            throw unmodifiableException();
        }

        @Override
        public int indexOf(final Object o) {
            return data.indexOf(o);
        }

        @Override
        public int lastIndexOf(final Object o) {
            return data.lastIndexOf(o);
        }

        @Override
        public ListIterator<Object> listIterator() {
            return new UnmodifiableListIterator(data.listIterator());
        }

        @Override
        public ListIterator<Object> listIterator(final int index) {
            return new UnmodifiableListIterator(data.listIterator(index));
        }

        @Override
        public List<Object> subList(final int fromIndex, final int toIndex) {
            return new UnmodifiableIngestList(data.subList(fromIndex, toIndex));
        }

        private static final class UnmodifiableListIterator implements ListIterator<Object> {

            private final ListIterator<Object> data;

            UnmodifiableListIterator(ListIterator<Object> data) {
                this.data = data;
            }

            @Override
            public boolean hasNext() {
                return data.hasNext();
            }

            @Override
            public Object next() {
                return wrapUnmodifiable(data.next());
            }

            @Override
            public boolean hasPrevious() {
                return data.hasPrevious();
            }

            @Override
            public Object previous() {
                return wrapUnmodifiable(data.previous());
            }

            @Override
            public int nextIndex() {
                return data.nextIndex();
            }

            @Override
            public int previousIndex() {
                return data.previousIndex();
            }

            @Override
            public void remove() {
                throw unmodifiableException();
            }

            @Override
            public void set(final Object o) {
                throw unmodifiableException();
            }

            @Override
            public void add(final Object o) {
                throw unmodifiableException();
            }
        }
    }
}
