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

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.ingest.ConfigurationUtils;

public final class DurationProcessor extends AbstractStringProcessor<Long> {

    public static final String TYPE = "duration";
    private static final String DEFAULT_UNIT = TimeUnit.MILLISECONDS.name();

    private final TimeUnit targetUnit;

    DurationProcessor(String tag, String field, boolean ignoreMissing, String targetField, TimeUnit targetUnit) {
        super(tag, field, ignoreMissing, targetField);
        this.targetUnit = targetUnit;
    }

    @Override
    protected Long process(String value) {
        try {
            TimeValue timeValue = TimeValue.parseTimeValue(value, getField());
            return targetUnit.convert(timeValue.duration(), timeValue.timeUnit());
        } catch (IllegalArgumentException iae) {
            throw new ElasticsearchParseException("failed to parse field [{}] with value [{}] as a time value", iae, getField(), value);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractStringProcessor.Factory {

        protected Factory() {
            super(TYPE);
        }

        @Override
        protected AbstractStringProcessor newProcessor(String processorTag, Map<String, Object> config, String field,
                                                       boolean ignoreMissing, String targetField) {
            final String rawTimeUnit = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_unit", DEFAULT_UNIT);
            TimeUnit toUnit = parseTimeUnit(rawTimeUnit, "target_unit");
            return new DurationProcessor(processorTag, field, ignoreMissing, targetField, toUnit);
        }

        private static TimeUnit parseTimeUnit(String unit, String fieldName) {
            try {
                return TimeUnit.valueOf(unit.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                throw new ElasticsearchParseException("failed to parse field [{}] with value [{}] as a time unit: Unrecognized time unit",
                    iae, fieldName, unit);
            }
        }
    }
}
