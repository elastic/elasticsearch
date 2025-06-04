/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

public final class HtmlStripProcessor extends AbstractStringProcessor<String> {

    public static final String TYPE = "html_strip";

    HtmlStripProcessor(String tag, String description, String field, boolean ignoreMissing, String targetField) {
        super(tag, description, ignoreMissing, targetField, field);
    }

    @Override
    protected String process(String value) {
        // shortcut, no need to create a string builder and go through each char
        if (value.contains("<") == false || value.contains(">") == false) {
            return value;
        }

        StringBuilder builder = new StringBuilder();
        try (HTMLStripCharFilter filter = new HTMLStripCharFilter(new StringReader(value))) {
            int ch;
            while ((ch = filter.read()) != -1) {
                builder.append((char) ch);
            }
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }

        return builder.toString();
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
        protected HtmlStripProcessor newProcessor(
            String tag,
            String description,
            Map<String, Object> config,
            String field,
            boolean ignoreMissing,
            String targetField
        ) {
            return new HtmlStripProcessor(tag, description, field, ignoreMissing, targetField);
        }
    }
}
