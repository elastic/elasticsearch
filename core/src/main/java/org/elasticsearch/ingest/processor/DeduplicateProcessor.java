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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.ConfigurationUtils;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;


/**
 * Processor that removes duplicate entries from an array.
 * Throws exception is the specified field is not an array.
 */
public final class DeduplicateProcessor extends AbstractProcessor {

    public static final String TYPE = "dedupe";
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField ORDERED = new ParseField("ordered");
    public static final boolean DEFAULT_ORDERED = true;

    private final String field;
    private final boolean ordered;

    DeduplicateProcessor(String tag, String field, boolean ordered) {
        super(tag);
        this.field = field;
        this.ordered = ordered;
    }

    String getField() {
        return field;
    }

    @Override
    public void execute(IngestDocument document) {
        List<?> list = document.getFieldValue(field, List.class);
        if (list == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot deduplicate.");
        }

        if (list.size() <= 1) {
            return;
        }

        if (lastType != null && lastType.equals("sort")) {
            list = dedupeSorted(list);
        } else {
            list = dedupeUnsorted(list, ordered);
        }

        document.setFieldValue(field, list);
    }

    private <T> List<T> dedupeSorted(List<T> list) {
        T last = null;
        ListIterator<T> iterator = list.listIterator();
        while (iterator.hasNext()) {
            T value = iterator.next();
            if (value.equals(last)) {
                iterator.remove();
            } else {
                last = value;
            }
        }

        return list;
    }

    private <T> List<T> dedupeUnsorted(List<T> list, boolean ordered) {
        Set<T> dedupeSet;
        if (ordered) {
            dedupeSet = new LinkedHashSet<>(list);
        } else {
            dedupeSet = new HashSet<>(list);
        }

        list.clear();
        list.addAll(dedupeSet);
        return list;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public final static class Factory extends AbstractProcessorFactory<DeduplicateProcessor> {
        @Override
        public DeduplicateProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, FIELD.getPreferredName());
            boolean ordered = ConfigurationUtils.readBooleanProperty(TYPE,
                processorTag,
                config,
                ORDERED.getPreferredName(),
                DEFAULT_ORDERED);
            return new DeduplicateProcessor(processorTag, field, ordered);
        }
    }
}

