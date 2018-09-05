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

import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public final class DissectProcessor extends AbstractProcessor {

    public static final String TYPE = "dissect";
    //package private members for testing
    final String field;
    final boolean ignoreMissing;
    final String pattern;
    final String appendSeparator;
    final DissectParser dissectParser;

    DissectProcessor(String tag, String field, String pattern, String appendSeparator, boolean ignoreMissing) {
        super(tag);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
        this.pattern = pattern;
        this.appendSeparator = appendSeparator;
        this.dissectParser = new DissectParser(pattern, appendSeparator);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        String input = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (input == null && ignoreMissing) {
            return ingestDocument;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        dissectParser.parse(input).forEach(ingestDocument::setFieldValue);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public DissectProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String pattern = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "pattern");
            String appendSeparator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "append_separator", "");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new DissectProcessor(processorTag, field, pattern, appendSeparator, ignoreMissing);
        }
    }
}
