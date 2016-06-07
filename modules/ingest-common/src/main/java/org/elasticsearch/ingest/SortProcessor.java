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

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.ConfigurationUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Processor that sorts an array of items.
 * Throws exception is the specified field is not an array.
 */
public final class SortProcessor extends AbstractProcessor {

    public static final String TYPE = "sort";
    public static final String FIELD = "field";
    public static final String ORDER = "order";
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
        List<? extends Comparable> list = document.getFieldValue(field, List.class);

        if (list == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot sort.");
        }

        if (list.size() <= 1) {
            return;
        }

        if (order.equals(SortOrder.ASCENDING)) {
            Collections.sort(list);
        } else {
            Collections.sort(list, Collections.reverseOrder());
        }

        document.setFieldValue(field, list);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public final static class Factory extends AbstractProcessorFactory<SortProcessor> {

        @Override
        public SortProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, FIELD);
            try {
                SortOrder direction = SortOrder.fromString(
                    ConfigurationUtils.readStringProperty(
                        TYPE,
                        processorTag,
                        config,
                        ORDER,
                        DEFAULT_ORDER));
                return new SortProcessor(processorTag, field, direction);
            } catch (IllegalArgumentException e) {
                throw ConfigurationUtils.newConfigurationException(TYPE, processorTag, ORDER, e.getMessage());
            }
        }
    }
}

