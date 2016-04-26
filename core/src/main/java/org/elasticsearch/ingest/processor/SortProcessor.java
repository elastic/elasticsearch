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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Processor that sorts an array of items.
 * Throws exception is the specified field is not an array.
 */
public final class SortProcessor extends AbstractProcessor {

    public static final String TYPE = "sort";
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField ORDER = new ParseField("order");
    public static final String DEFAULT_ORDER = "asc";

    public enum SortOrder {
        ASCENDING("asc"), DESCENDING("desc");

        private final String direction;

        SortOrder(String direction) {
            this.direction = direction;
        }

        public String toString() {
            return this.direction;
        }

        public static SortOrder fromString(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Sort direction cannot be null");
            }

            if (value.equals(ASCENDING.toString())) {
                return ASCENDING;
            } else if (value.equals(DESCENDING.toString())) {
                return DESCENDING;
            }
            throw new IllegalArgumentException("Sort direction [" + value + "] not recognized."
                    + " Valid values are: [asc, desc]");
        }
    }

    private final String field;
    private final SortOrder order;

    SortProcessor(String tag, String field, SortOrder order) {
        super(tag);
        this.field = field;
        this.order = order;
    }

    String getField() {
        return field;
    }

    SortOrder getOrder() {
        return order;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(IngestDocument document) {
        List<?> list = document.getFieldValue(field, List.class);

        if (list == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot sort.");
        }

        if (list.size() <= 1) {
            return;
        }

        Object first = list.get(0);
        Comparator comparator;

        // TODO
        // This all feels awful, is there a better way?  I had to do it this way
        // so we can chain a .reverse() later, otherwise the intrinsic
        // .stream().sorted()... would have worked fine
        if (first instanceof Double) {
            comparator = Comparator.comparingDouble(d -> (Double)d);
        } else if (first instanceof Float) {
            comparator = (Object o1, Object o2) -> ((Float)o1).compareTo((Float)o2);
        } else if (first instanceof Long) {
            comparator = Comparator.comparingLong(d -> (Long)d);
        } else if (first instanceof Integer) {
            comparator = Comparator.comparingInt(d -> (Integer)d);
        } else if (first instanceof Number) {
            // catchall for shorts, bytes, etc.
            // TODO gross, cleaner way?
            comparator =
                (Object o1, Object o2) -> ((Number)o1).intValue() - ((Number)o2).intValue();
        } else if (first instanceof Boolean) {
            comparator = (Object o1, Object o2) -> ((Boolean)o1).compareTo((Boolean)o2);
        } else {
            comparator = Comparator.comparing(String::valueOf);
        }

        if (order.equals(SortOrder.DESCENDING)) {
            comparator = comparator.reversed();
        }

        Collections.sort(list, comparator);
        document.setFieldValue(field, list);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public final static class Factory extends AbstractProcessorFactory<SortProcessor> {

        @Override
        public SortProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, FIELD.getPreferredName());
            SortOrder direction = SortOrder.fromString(
                    ConfigurationUtils.readStringProperty(
                            TYPE,
                            processorTag,
                            config,
                            ORDER.getPreferredName(),
                        DEFAULT_ORDER));

            return new SortProcessor(processorTag, field, direction);
        }
    }
}

