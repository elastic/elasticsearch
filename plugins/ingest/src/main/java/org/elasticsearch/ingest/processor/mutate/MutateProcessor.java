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

package org.elasticsearch.ingest.processor.mutate;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class MutateProcessor implements Processor {

    public static final String TYPE = "mutate";

    private final Map<String, Object> update;
    private final Map<String, String> rename;
    private final Map<String, String> convert;
    private final Map<String, String> split;
    private final List<GsubExpression> gsub;
    private final Map<String, String> join;
    private final List<String> remove;
    private final List<String> trim;
    private final List<String> uppercase;
    private final List<String> lowercase;

    MutateProcessor(Map<String, Object> update, Map<String, String> rename, Map<String, String> convert,
                           Map<String, String> split, List<GsubExpression> gsub, Map<String, String> join,
                           List<String> remove, List<String> trim, List<String> uppercase, List<String> lowercase) {
        this.update = update;
        this.rename = rename;
        this.convert = convert;
        this.split = split;
        this.gsub = gsub;
        this.join = join;
        this.remove = remove;
        this.trim = trim;
        this.uppercase = uppercase;
        this.lowercase = lowercase;
    }

    Map<String, Object> getUpdate() {
        return update;
    }

    Map<String, String> getRename() {
        return rename;
    }

    Map<String, String> getConvert() {
        return convert;
    }

    Map<String, String> getSplit() {
        return split;
    }

    List<GsubExpression> getGsub() {
        return gsub;
    }

    Map<String, String> getJoin() {
        return join;
    }

    List<String> getRemove() {
        return remove;
    }

    List<String> getTrim() {
        return trim;
    }

    List<String> getUppercase() {
        return uppercase;
    }

    List<String> getLowercase() {
        return lowercase;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        if (update != null) {
            doUpdate(ingestDocument);
        }
        if (rename != null) {
            doRename(ingestDocument);
        }
        if (convert != null) {
            doConvert(ingestDocument);
        }
        if (split != null) {
            doSplit(ingestDocument);
        }
        if (gsub != null) {
            doGsub(ingestDocument);
        }
        if (join != null) {
            doJoin(ingestDocument);
        }
        if (remove != null) {
            doRemove(ingestDocument);
        }
        if (trim != null) {
            doTrim(ingestDocument);
        }
        if (uppercase != null) {
            doUppercase(ingestDocument);
        }
        if (lowercase != null) {
            doLowercase(ingestDocument);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private void doUpdate(IngestDocument ingestDocument) {
        for(Map.Entry<String, Object> entry : update.entrySet()) {
            ingestDocument.setPropertyValue(entry.getKey(), entry.getValue());
        }
    }

    private void doRename(IngestDocument ingestDocument) {
        for(Map.Entry<String, String> entry : rename.entrySet()) {
            if (ingestDocument.hasPropertyValue(entry.getKey())) {
                Object oldVal = ingestDocument.getPropertyValue(entry.getKey(), Object.class);
                ingestDocument.getSource().remove(entry.getKey());
                ingestDocument.setPropertyValue(entry.getValue(), oldVal);
            }
        }
    }

    private Object parseValueAsType(Object oldVal, String toType) {
        switch (toType) {
            case "integer":
                oldVal = Integer.parseInt(oldVal.toString());
                break;
            case "float":
                oldVal = Float.parseFloat(oldVal.toString());
                break;
            case "string":
                oldVal = oldVal.toString();
                break;
            case "boolean":
                // TODO(talevy): Booleans#parseBoolean depends on Elasticsearch, should be moved into dedicated library.
                oldVal = Booleans.parseBoolean(oldVal.toString(), false);
        }

        return oldVal;
    }

    @SuppressWarnings("unchecked")
    private void doConvert(IngestDocument ingestDocument) {
        for(Map.Entry<String, String> entry : convert.entrySet()) {
            String toType = entry.getValue();

            Object oldVal = ingestDocument.getPropertyValue(entry.getKey(), Object.class);
            Object newVal;

            if (oldVal instanceof List) {
                newVal = new ArrayList<>();
                for (Object e : ((List<Object>) oldVal)) {
                    ((List<Object>) newVal).add(parseValueAsType(e, toType));
                }
            } else {
                if (oldVal == null) {
                    throw new IllegalArgumentException("Field \"" + entry.getKey() + "\" is null, cannot be converted to a/an " + toType);
                }
                newVal = parseValueAsType(oldVal, toType);
            }

            ingestDocument.setPropertyValue(entry.getKey(), newVal);
        }
    }

    private void doSplit(IngestDocument ingestDocument) {
        for(Map.Entry<String, String> entry : split.entrySet()) {
            Object oldVal = ingestDocument.getPropertyValue(entry.getKey(), Object.class);
            if (oldVal == null) {
                throw new IllegalArgumentException("Cannot split field. [" + entry.getKey() + "] is null.");
            } else if (oldVal instanceof String) {
                ingestDocument.setPropertyValue(entry.getKey(), Arrays.asList(((String) oldVal).split(entry.getValue())));
            } else {
                throw new IllegalArgumentException("Cannot split a field that is not a String type");
            }
        }
    }

    private void doGsub(IngestDocument ingestDocument) {
        for (GsubExpression gsubExpression : gsub) {
            String oldVal = ingestDocument.getPropertyValue(gsubExpression.getFieldName(), String.class);
            if (oldVal == null) {
                throw new IllegalArgumentException("Field \"" + gsubExpression.getFieldName() + "\" is null, cannot match pattern.");
            }
            Matcher matcher = gsubExpression.getPattern().matcher(oldVal);
            String newVal = matcher.replaceAll(gsubExpression.getReplacement());
            ingestDocument.setPropertyValue(gsubExpression.getFieldName(), newVal);
        }
    }

    @SuppressWarnings("unchecked")
    private void doJoin(IngestDocument ingestDocument) {
        for(Map.Entry<String, String> entry : join.entrySet()) {
            Object oldVal = ingestDocument.getPropertyValue(entry.getKey(), Object.class);
            if (oldVal instanceof List) {
                String joined = (String) ((List) oldVal)
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(entry.getValue()));

                ingestDocument.setPropertyValue(entry.getKey(), joined);
            } else {
                throw new IllegalArgumentException("Cannot join field:" + entry.getKey() + " with type: " + oldVal.getClass());
            }
        }
    }

    private void doRemove(IngestDocument ingestDocument) {
        for(String field : remove) {
            ingestDocument.getSource().remove(field);
        }
    }

    private void doTrim(IngestDocument ingestDocument) {
        for(String field : trim) {
            Object val = ingestDocument.getPropertyValue(field, Object.class);
            if (val == null) {
                throw new IllegalArgumentException("Cannot trim field. [" + field + "] is null.");
            } else if (val instanceof String) {
                ingestDocument.setPropertyValue(field, ((String) val).trim());
            } else {
                throw new IllegalArgumentException("Cannot trim field:" + field + " with type: " + val.getClass());
            }
        }
    }

    private void doUppercase(IngestDocument ingestDocument) {
        for(String field : uppercase) {
            Object val = ingestDocument.getPropertyValue(field, Object.class);
            if (val == null) {
                throw new IllegalArgumentException("Cannot uppercase field. [" + field + "] is null.");
            } else if (val instanceof String) {
                ingestDocument.setPropertyValue(field, ((String) val).toUpperCase(Locale.ROOT));
            } else {
                throw new IllegalArgumentException("Cannot uppercase field:" + field + " with type: " + val.getClass());
            }
        }
    }

    private void doLowercase(IngestDocument ingestDocument) {
        for(String field : lowercase) {
            Object val = ingestDocument.getPropertyValue(field, Object.class);
            if (val == null) {
                throw new IllegalArgumentException("Cannot lowercase field. [" + field + "] is null.");
            } else if (val instanceof String) {
                ingestDocument.setPropertyValue(field, ((String) val).toLowerCase(Locale.ROOT));
            } else {
                throw new IllegalArgumentException("Cannot lowercase field:" + field + " with type: " + val.getClass());
            }
        }
    }

    public static final class Factory implements Processor.Factory<MutateProcessor> {
        @Override
        public MutateProcessor create(Map<String, Object> config) throws IOException {
            Map<String, Object> update = ConfigurationUtils.readOptionalMap(config, "update");
            Map<String, String> rename = ConfigurationUtils.readOptionalMap(config, "rename");
            Map<String, String> convert = ConfigurationUtils.readOptionalMap(config, "convert");
            Map<String, String> split = ConfigurationUtils.readOptionalMap(config, "split");
            Map<String, List<String>> gsubConfig = ConfigurationUtils.readOptionalMap(config, "gsub");
            Map<String, String> join = ConfigurationUtils.readOptionalMap(config, "join");
            List<String> remove = ConfigurationUtils.readOptionalList(config, "remove");
            List<String> trim = ConfigurationUtils.readOptionalList(config, "trim");
            List<String> uppercase = ConfigurationUtils.readOptionalList(config, "uppercase");
            List<String> lowercase = ConfigurationUtils.readOptionalList(config, "lowercase");

            // pre-compile regex patterns
            List<GsubExpression> gsubExpressions = null;
            if (gsubConfig != null) {
                gsubExpressions = new ArrayList<>();
                for (Map.Entry<String, List<String>> entry : gsubConfig.entrySet()) {
                    List<String> searchAndReplace = entry.getValue();
                    if (searchAndReplace.size() != 2) {
                        throw new IllegalArgumentException("Invalid search and replace values " + searchAndReplace + " for field: " + entry.getKey());
                    }
                    Pattern searchPattern = Pattern.compile(searchAndReplace.get(0));
                    gsubExpressions.add(new GsubExpression(entry.getKey(), searchPattern, searchAndReplace.get(1)));
                }
            }

            return new MutateProcessor(
                    (update == null) ? null : Collections.unmodifiableMap(update),
                    (rename == null) ? null : Collections.unmodifiableMap(rename),
                    (convert == null) ? null : Collections.unmodifiableMap(convert),
                    (split == null) ? null : Collections.unmodifiableMap(split),
                    (gsubExpressions == null) ? null : Collections.unmodifiableList(gsubExpressions),
                    (join == null) ? null : Collections.unmodifiableMap(join),
                    (remove == null) ? null : Collections.unmodifiableList(remove),
                    (trim == null) ? null : Collections.unmodifiableList(trim),
                    (uppercase == null) ? null : Collections.unmodifiableList(uppercase),
                    (lowercase == null) ? null : Collections.unmodifiableList(lowercase));
        }
    }
}
