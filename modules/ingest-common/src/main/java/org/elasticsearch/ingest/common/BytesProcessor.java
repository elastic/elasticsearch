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

import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Map;

/**
 * Processor that converts the content of string fields to the byte value.
 * Throws exception is the field is not of type string or can not convert to the numeric byte value
 */
public final class BytesProcessor extends AbstractStringProcessor<Long> {

    public static final String TYPE = "bytes";

    BytesProcessor(String processorTag, String description, String field, boolean ignoreMissing, String targetField) {
        super(processorTag, description, ignoreMissing, targetField, field);
    }

    public static long apply(String value) {
        return ByteSizeValue.parseBytesSizeValue(value, null, "Ingest Field").getBytes();
    }

    @Override
    protected Long process(String value) {
        return apply(value);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractStringProcessor.Factory {

        public Factory() {
            super(TYPE);
        }

        @Override
        protected BytesProcessor newProcessor(String tag, String description, Map<String, Object> config, String field,
                                              boolean ignoreMissing, String targetField) {
            return new BytesProcessor(tag, description, field, ignoreMissing, targetField);
        }
    }
}
