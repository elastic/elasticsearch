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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Processor that URL-decodes a string
 */
public final class URLDecodeProcessor extends AbstractStringProcessor {

    public static final String TYPE = "urldecode";

    URLDecodeProcessor(String processorTag, String field, boolean ignoreMissing, String targetField) {
        super(processorTag, field, ignoreMissing, targetField);
    }

    @Override
    protected String process(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("could not URL-decode field[" + getField() + "]", e);
        }
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
        protected URLDecodeProcessor newProcessor(String tag, Map<String, Object> config, String field,
                                                  boolean ignoreMissing, String targetField) {
            return new URLDecodeProcessor(tag, field, ignoreMissing, targetField);
        }
    }
}
