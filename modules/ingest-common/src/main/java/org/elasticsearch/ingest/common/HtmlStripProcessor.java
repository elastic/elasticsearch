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

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.elasticsearch.ElasticsearchException;

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
        protected HtmlStripProcessor newProcessor(String tag, String description, Map<String, Object> config, String field,
                                             boolean ignoreMissing, String targetField) {
            return new HtmlStripProcessor(tag, description, field, ignoreMissing, targetField);
        }
    }
}
