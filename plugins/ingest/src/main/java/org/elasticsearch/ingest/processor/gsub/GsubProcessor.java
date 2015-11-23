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

package org.elasticsearch.ingest.processor.gsub;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Processor that allows to search for patterns in field content and replace them with corresponding string replacement.
 * Support fields of string type only, throws exception if a field is of a different type.
 */
public class GsubProcessor implements Processor {

    public static final String TYPE = "gsub";

    private final List<GsubExpression> gsubExpressions;

    GsubProcessor(List<GsubExpression> gsubExpressions) {
        this.gsubExpressions = gsubExpressions;
    }

    List<GsubExpression> getGsubExpressions() {
        return gsubExpressions;
    }

    @Override
    public void execute(IngestDocument document) {
        for (GsubExpression gsubExpression : gsubExpressions) {
            String oldVal = document.getPropertyValue(gsubExpression.getFieldName(), String.class);
            if (oldVal == null) {
                throw new IllegalArgumentException("field [" + gsubExpression.getFieldName() + "] is null, cannot match pattern.");
            }
            Matcher matcher = gsubExpression.getPattern().matcher(oldVal);
            String newVal = matcher.replaceAll(gsubExpression.getReplacement());
            document.setPropertyValue(gsubExpression.getFieldName(), newVal);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<GsubProcessor> {
        @Override
        public GsubProcessor create(Map<String, Object> config) throws IOException {
            List<Map<String, String>> gsubConfig = ConfigurationUtils.readList(config, "expressions");
            List<GsubExpression> gsubExpressions = new ArrayList<>();
            for (Map<String, String> stringObjectMap : gsubConfig) {
                String field = stringObjectMap.get("field");
                if (field == null) {
                    throw new IllegalArgumentException("no [field] specified for gsub expression");
                }
                String pattern = stringObjectMap.get("pattern");
                if (pattern == null) {
                    throw new IllegalArgumentException("no [pattern] specified for gsub expression");
                }
                String replacement = stringObjectMap.get("replacement");
                if (replacement == null) {
                    throw new IllegalArgumentException("no [replacement] specified for gsub expression");
                }
                Pattern searchPattern = Pattern.compile(pattern);
                gsubExpressions.add(new GsubExpression(field, searchPattern, replacement));
            }
            return new GsubProcessor(gsubExpressions);
        }
    }
}
