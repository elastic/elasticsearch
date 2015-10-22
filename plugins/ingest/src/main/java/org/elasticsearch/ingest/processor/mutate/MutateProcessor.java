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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.ingest.Data;
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
    private final Map<String, Tuple<Pattern, String>> gsub;
    private final Map<String, String> join;
    private final List<String> remove;
    private final List<String> trim;
    private final List<String> uppercase;
    private final List<String> lowercase;

    public MutateProcessor(Map<String, Object> update,
                           Map<String, String> rename,
                           Map<String, String> convert,
                           Map<String, String> split,
                           Map<String, Tuple<Pattern, String>> gsub,
                           Map<String, String> join,
                           List<String> remove,
                           List<String> trim,
                           List<String> uppercase,
                           List<String> lowercase) {
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

    public Map<String, Object> getUpdate() {
        return update;
    }

    public Map<String, String> getRename() {
        return rename;
    }

    public Map<String, String> getConvert() {
        return convert;
    }

    public Map<String, String> getSplit() {
        return split;
    }

    public Map<String, Tuple<Pattern, String>> getGsub() {
        return gsub;
    }

    public Map<String, String> getJoin() {
        return join;
    }

    public List<String> getRemove() {
        return remove;
    }

    public List<String> getTrim() {
        return trim;
    }

    public List<String> getUppercase() {
        return uppercase;
    }

    public List<String> getLowercase() {
        return lowercase;
    }

    @Override
    public void execute(Data data) {
        if (update != null) {
            doUpdate(data);
        }
        if (rename != null) {
            doRename(data);
        }
        if (convert != null) {
            doConvert(data);
        }
        if (split != null) {
            doSplit(data);
        }
        if (gsub != null) {
            doGsub(data);
        }
        if (join != null) {
            doJoin(data);
        }
        if (remove != null) {
            doRemove(data);
        }
        if (trim != null) {
            doTrim(data);
        }
        if (uppercase != null) {
            doUppercase(data);
        }
        if (lowercase != null) {
            doLowercase(data);
        }
    }

    private void doUpdate(Data data) {
        for(Map.Entry<String, Object> entry : update.entrySet()) {
            data.addField(entry.getKey(), entry.getValue());
        }
    }

    private void doRename(Data data) {
        for(Map.Entry<String, String> entry : rename.entrySet()) {
            if (data.containsProperty(entry.getKey())) {
                Object oldVal = data.getProperty(entry.getKey());
                data.getDocument().remove(entry.getKey());
                data.addField(entry.getValue(), oldVal);
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
    private void doConvert(Data data) {
        for(Map.Entry<String, String> entry : convert.entrySet()) {
            String toType = entry.getValue();

            Object oldVal = data.getProperty(entry.getKey());
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

            data.addField(entry.getKey(), newVal);
        }
    }

    private void doSplit(Data data) {
        for(Map.Entry<String, String> entry : split.entrySet()) {
            Object oldVal = data.getProperty(entry.getKey());
            if (oldVal instanceof String) {
                data.addField(entry.getKey(), Arrays.asList(((String) oldVal).split(entry.getValue())));
            } else {
                throw new IllegalArgumentException("Cannot split a field that is not a String type");
            }
        }
    }

    private void doGsub(Data data) {
        for (Map.Entry<String, Tuple<Pattern, String>> entry : gsub.entrySet()) {
            String fieldName = entry.getKey();
            Tuple<Pattern, String> matchAndReplace = entry.getValue();
            String oldVal = data.getProperty(fieldName);
            if (oldVal == null) {
                throw new IllegalArgumentException("Field \"" + fieldName + "\" is null, cannot match pattern.");
            }
            Matcher matcher = matchAndReplace.v1().matcher(oldVal);
            String newVal = matcher.replaceAll(matchAndReplace.v2());
            data.addField(entry.getKey(), newVal);
        }
    }

    @SuppressWarnings("unchecked")
    private void doJoin(Data data) {
        for(Map.Entry<String, String> entry : join.entrySet()) {
            Object oldVal = data.getProperty(entry.getKey());
            if (oldVal instanceof List) {
                String joined = (String) ((List) oldVal)
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(entry.getValue()));

                data.addField(entry.getKey(), joined);
            } else {
                throw new IllegalArgumentException("Cannot join field:" + entry.getKey() + " with type: " + oldVal.getClass());
            }
        }
    }

    private void doRemove(Data data) {
        for(String field : remove) {
            data.getDocument().remove(field);
        }
    }

    private void doTrim(Data data) {
        for(String field : trim) {
            Object val = data.getProperty(field);
            if (val instanceof String) {
                data.addField(field, ((String) val).trim());
            } else {
                throw new IllegalArgumentException("Cannot trim field:" + field + " with type: " + val.getClass());
            }
        }
    }

    private void doUppercase(Data data) {
        for(String field : uppercase) {
            Object val = data.getProperty(field);
            if (val instanceof String) {
                data.addField(field, ((String) val).toUpperCase(Locale.ROOT));
            } else {
                throw new IllegalArgumentException("Cannot uppercase field:" + field + " with type: " + val.getClass());
            }
        }
    }

    private void doLowercase(Data data) {
        for(String field : lowercase) {
            Object val = data.getProperty(field);
            if (val instanceof String) {
                data.addField(field, ((String) val).toLowerCase(Locale.ROOT));
            } else {
                throw new IllegalArgumentException("Cannot lowercase field:" + field + " with type: " + val.getClass());
            }
        }
    }

    public static final class Factory implements Processor.Factory<MutateProcessor> {
        @Override
        public MutateProcessor create(Map<String, Object> config) throws IOException {
            Map<String, Object> update = ConfigurationUtils.readOptionalObjectMap(config, "update");
            Map<String, String> rename = ConfigurationUtils.readOptionalStringMap(config, "rename");
            Map<String, String> convert = ConfigurationUtils.readOptionalStringMap(config, "convert");
            Map<String, String> split = ConfigurationUtils.readOptionalStringMap(config, "split");
            Map<String, List<String>> gsubConfig = ConfigurationUtils.readOptionalStringListMap(config, "gsub");
            Map<String, String> join = ConfigurationUtils.readOptionalStringMap(config, "join");
            List<String> remove = ConfigurationUtils.readOptionalStringList(config, "remove");
            List<String> trim = ConfigurationUtils.readOptionalStringList(config, "trim");
            List<String> uppercase = ConfigurationUtils.readOptionalStringList(config, "uppercase");
            List<String> lowercase = ConfigurationUtils.readOptionalStringList(config, "lowercase");

            // pre-compile regex patterns
            Map<String, Tuple<Pattern, String>> gsub = null;
            if (gsubConfig != null) {
                gsub = new HashMap<>();
                for (Map.Entry<String, List<String>> entry : gsubConfig.entrySet()) {
                    List<String> searchAndReplace = entry.getValue();
                    if (searchAndReplace.size() != 2) {
                        throw new IllegalArgumentException("Invalid search and replace values (" + Arrays.toString(searchAndReplace.toArray()) + ") for field: " + entry.getKey());
                    }
                    Pattern searchPattern = Pattern.compile(searchAndReplace.get(0));
                    gsub.put(entry.getKey(), new Tuple<>(searchPattern, searchAndReplace.get(1)));
                }
            }

            return new MutateProcessor(
                    (update == null) ? null : Collections.unmodifiableMap(update),
                    (rename == null) ? null : Collections.unmodifiableMap(rename),
                    (convert == null) ? null : Collections.unmodifiableMap(convert),
                    (split == null) ? null : Collections.unmodifiableMap(split),
                    (gsub == null) ? null : Collections.unmodifiableMap(gsub),
                    (join == null) ? null : Collections.unmodifiableMap(join),
                    (remove == null) ? null : Collections.unmodifiableList(remove),
                    (trim == null) ? null : Collections.unmodifiableList(trim),
                    (uppercase == null) ? null : Collections.unmodifiableList(uppercase),
                    (lowercase == null) ? null : Collections.unmodifiableList(lowercase));
        }
    }
}
