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

package org.elasticsearch.ingest.common;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ProcessorConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readMap;

public class ConditionalProcessor extends AbstractProcessor {

    static final String TYPE = "conditional";

    private final Script condition;

    private final ScriptService scriptService;

    private final Processor processor;

    private ConditionalProcessor(String tag, Script script, ScriptService scriptService, Processor processor) {
        super(tag);
        this.condition = script;
        this.scriptService = scriptService;
        this.processor = processor;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        if (scriptService.compile(condition, ProcessorConditionalScript.CONTEXT)
            .newInstance(condition.getParams()).execute(new UnmodifiableIngestData(ingestDocument.getSourceAndMetadata()))) {
            processor.execute(ingestDocument);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ConditionalProcessor create(Map<String, Processor.Factory> factories, String tag,
            Map<String, Object> config) throws Exception {
            Map<String, Map<String, Object>> processorConfig = readMap(TYPE, tag, config, "processor");
            final Script script;
            try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)
                .map(normalizeScript(config.get("script")));
                 InputStream stream = BytesReference.bytes(builder).streamInput();
                 XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
                script = Script.parse(parser);
                config.remove("script");
                // verify script is able to be compiled before successfully creating processor.
                try {
                    scriptService.compile(script, ProcessorConditionalScript.CONTEXT);
                } catch (ScriptException e) {
                    throw newConfigurationException(TYPE, tag, null, e);
                }
            }
            Map.Entry<String, Map<String, Object>> entry = processorConfig.entrySet().iterator().next();
            Processor processor = ConfigurationUtils.readProcessor(factories, entry.getKey(), entry.getValue());
            return new ConditionalProcessor(tag, script, scriptService, processor);
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> normalizeScript(Object scriptConfig) {
            if (scriptConfig instanceof Map<?, ?>) {
                return (Map<String, Object>) scriptConfig;
            } else if (scriptConfig instanceof String) {
                return Collections.singletonMap("source", scriptConfig);
            } else {
                throw newConfigurationException(TYPE, null, "script",
                    "property isn't a map or string, but of type [" + scriptConfig.getClass().getName() + "]");
            }
        }
    }

    private static Object wrapUnmodifiable(Object raw) {
        if (raw instanceof Map) {
            return new UnmodifiableIngestData((Map<String, Object>) raw);
        } else if (raw instanceof List) {
            return new UnmodifiableIngestList((List<Object>) raw);
        } else if (raw instanceof byte[]) {
            return ((byte[]) raw).clone();
        } else if (raw instanceof Date) {
            return ((Date) raw).clone();
        }
        return raw;
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
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public Object remove(final Object key) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public void putAll(final Map<? extends String, ?> m) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
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
            throw new UnsupportedOperationException("Getting EntrySet for ingest documents in conditionals is not supported");
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
                    throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
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
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public boolean remove(final Object o) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return data.contains(c);
        }

        @Override
        public boolean addAll(final Collection<?> c) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public boolean addAll(final int index, final Collection<?> c) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public boolean removeAll(final Collection<?> c) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public boolean retainAll(final Collection<?> c) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public Object get(final int index) {
            return wrapUnmodifiable(data.get(index));
        }

        @Override
        public Object set(final int index, final Object element) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public void add(final int index, final Object element) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
        }

        @Override
        public Object remove(final int index) {
            throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
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
                throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
            }

            @Override
            public void set(final Object o) {
                throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
            }

            @Override
            public void add(final Object o) {
                throw new UnsupportedOperationException("Mutating ingest documents in conditionals is not supported");
            }
        }
    }
}
