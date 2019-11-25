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

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;

import java.util.List;
import java.util.Map;

/**
 * A processor that breaks line from CSV file into separate fields.
 * If there's more fields requested than there is in the CSV, extra field will not be present in the document after processing.
 * In the same way this processor will skip any field that is empty in CSV.
 * <p>
 * <p>
 * By default it uses rules according to <a href="https://tools.ietf.org/html/rfc4180">RCF 4180</a>,
 * but it can be tweaked with following parameters:
 * <p>
 * quote: set custom quote character (defaults to ")
 * separator: set custom separator (defaults to ,)
 * trim: trim leading whitespaces in each field (allows also whitespaces before quoted fields, defaults to false)
 */
public final class CsvProcessor extends AbstractProcessor {

    public static final String TYPE = "csv";

    private final String field;
    private final String[] headers;
    private final boolean trim;
    private final char quote;
    private final char separator;
    private final boolean ignoreMissing;

    CsvProcessor(String tag, String field, String[] headers, boolean trim, char separator, char quote, boolean ignoreMissing) {
        super(tag);
        this.field = field;
        this.headers = headers;
        this.trim = trim;
        this.quote = quote;
        this.separator = separator;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        if (headers.length == 0) {
            return ingestDocument;
        }

        String line = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (line == null && ignoreMissing == false) {
            return ingestDocument;
        } else if (line == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        new CsvParser(ingestDocument, quote, separator, trim, headers).process(line);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements org.elasticsearch.ingest.Processor.Factory {
        @Override
        public CsvProcessor create(Map<String, org.elasticsearch.ingest.Processor.Factory> registry, String processorTag,
                                   Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String quote = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "quote", "\"");
            if (quote.length() != 1) {
                throw new IllegalArgumentException("quote has to be single character like \" or '");
            }
            String separator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "separator", ",");
            if (separator.length() != 1) {
                throw new IllegalArgumentException("separator has to be single character like \" or '");
            }
            boolean trim = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "trim", false);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            List<String> targetFields = ConfigurationUtils.readList(TYPE, processorTag, config, "target_fields");
            if (targetFields.isEmpty()) {
                throw new IllegalArgumentException("target fields list can't be empty");
            }
            return new CsvProcessor(processorTag, field, targetFields.toArray(String[]::new), trim, separator.charAt(0), quote.charAt(0),
                ignoreMissing);
        }
    }
}
