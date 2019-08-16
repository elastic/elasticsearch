/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.DeprecationMap;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class ConditionalProcessor extends AbstractProcessor implements WrappingProcessor {

    private static final Map<String, String> DEPRECATIONS =
            Map.of("_type", "[types removal] Looking up doc types [_type] in scripts is deprecated.");

    static final String TYPE = "conditional";

    private final Script condition;

    private final ScriptService scriptService;

    private final Processor processor;
    private final IngestMetric metric;
    private final LongSupplier relativeTimeProvider;

    ConditionalProcessor(String tag, Script script, ScriptService scriptService, Processor processor) {
        this(tag, script, scriptService, processor, System::nanoTime);
    }

    ConditionalProcessor(String tag, Script script, ScriptService scriptService, Processor processor, LongSupplier relativeTimeProvider) {
        super(tag);
        this.condition = script;
        this.scriptService = scriptService;
        this.processor = processor;
        this.metric = new IngestMetric();
        this.relativeTimeProvider = relativeTimeProvider;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        if (evaluate(ingestDocument)) {
            long startTimeInNanos = relativeTimeProvider.getAsLong();
            try {
                metric.preIngest();
                return processor.execute(ingestDocument);
            } catch (Exception e) {
                metric.ingestFailed();
                throw e;
            } finally {
                long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTimeInNanos);
                metric.postIngest(ingestTimeInMillis);
            }
        }
        return ingestDocument;
    }

    boolean evaluate(IngestDocument ingestDocument) {
        IngestConditionalScript script =
            scriptService.compile(condition, IngestConditionalScript.CONTEXT).newInstance(condition.getParams());
        return script.execute(new UnmodifiableIngestData(
                new DeprecationMap(ingestDocument.getSourceAndMetadata(), DEPRECATIONS, "conditional-processor")));
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
            return data.entrySet().stream().map(entry ->
                new Entry<String, Object>() {
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
