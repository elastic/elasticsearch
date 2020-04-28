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

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

/**
 * Processor that generates fingerprint from field names and values and write it
 * to target field. If the target field is already present, its value will be
 * replaced with the fingerprint.
 */
public final class FingerprintProcessor extends AbstractProcessor {

    public static final String TYPE = "fingerprint";

    private final Set<String> fields;
    private final String targetField;
    private final Method method;
    private final boolean base64Encode;
    private final boolean concatenateAllFields;
    private final boolean ignoreMissing;

    FingerprintProcessor(String tag, Set<String> fields, String targetField, Method method, boolean base64Encode,
            boolean concatenateAllFields, boolean ignoreMissing) {
        super(tag);
        this.fields = fields;
        this.targetField = targetField;
        this.method = method;
        this.base64Encode = base64Encode;
        this.concatenateAllFields = concatenateAllFields;
        this.ignoreMissing = ignoreMissing;
    }

    boolean isBase64Encode() {
        return base64Encode;
    }

    boolean isConcatenateAllFields() {
        return concatenateAllFields;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        if (method.equals(Method.UUID)) {
            // Generating UUID doesn't rely on document
            ingestDocument.setFieldValue(targetField, UUIDs.base64UUID());
            return ingestDocument;
        }
        // Get content from document that is used to generate fingerprint.
        byte[] content;
        if (concatenateAllFields) {
            content = ingestDocument.getSourceAndMetadata().toString().getBytes(StandardCharsets.UTF_8);
        } else {
            Map<String, Object> m = new HashMap<>();
            for (String field : fields) {
                Object value = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);
                if (value == null) {
                    if (!ignoreMissing) {
                        throw new IllegalArgumentException(
                                "field [" + field + "] is null, cannot generate fingerprint from it.");
                    }
                } else {
                    m.put(field, value);
                }
            }
            // All fields are missing, just ignore this document.
            if (m.isEmpty()) {
                return ingestDocument;
            }
            content = m.toString().getBytes(StandardCharsets.UTF_8);
        }

        // Generate fingerprint from content using specified hash function.
        byte[] digest;
        switch (method) {
        case MD5:
            digest = MessageDigests.md5().digest(content);
            break;
        case SHA256:
            digest = MessageDigests.sha256().digest(content);
            break;
        case SHA1:
            digest = MessageDigests.sha1().digest(content);
            break;
        case MURMUR3:
            MurmurHash3.Hash128 h = MurmurHash3.hash128(content, 0, content.length, 17, new MurmurHash3.Hash128());
            digest = new byte[16];
            System.arraycopy(Numbers.longToBytes(h.h1), 0, digest, 0, 8);
            System.arraycopy(Numbers.longToBytes(h.h2), 0, digest, 8, 8);
            break;
        default:
            throw new IllegalArgumentException("Invalid method: " + method.toString());
        }
        String targetValue;
        if (base64Encode) {
            targetValue = Base64.getEncoder().encodeToString(digest);
        } else {
            targetValue = MessageDigests.toHexString(digest);
        }
        // Set generated fingerprint to target field
        ingestDocument.setFieldValue(targetField, targetValue);

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    Set<String> getFields() {
        return fields;
    }

    String getTargetField() {
        return targetField;
    }

    Method getMethod() {
        return method;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public FingerprintProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                Map<String, Object> config) {
            List<String> fields = readOptionalList(TYPE, processorTag, config, "fields");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "fingerprint");
            String method = readStringProperty(TYPE, processorTag, config, "method", "MD5");
            boolean base64encode = readBooleanProperty(TYPE, processorTag, config, "base64_encode", false);
            boolean concatenateAllFields = readBooleanProperty(TYPE, processorTag, config, "concatenate_all_fields",
                    false);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            Method m;
            try {
                m = Method.parse(method);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, processorTag, "method",
                        "illegal value [" + method + "]. Valid values are " + Arrays.toString(Method.values()));
            }

            if (m.equals(Method.UUID)) {
                if (fields != null || concatenateAllFields) {
                    throw newConfigurationException(TYPE, processorTag, "method",
                            "is [" + method + "], [fields] or [concatenate_all_fields] must not be set");
                }
            } else {
                if (fields == null && !concatenateAllFields) {
                    throw newConfigurationException(TYPE, processorTag, "method",
                            "is [" + method + "], one of [fields] and [concatenate_all_fields] must be set");
                }
                if (fields != null && concatenateAllFields) {
                    throw newConfigurationException(TYPE, processorTag, "fields",
                            "can not be set when [concatenate_all_fields] is true");
                }
                if (fields != null && fields.isEmpty()) {
                    throw newConfigurationException(TYPE, processorTag, "fields", "can not be empty");
                }
            }

            Set<String> fieldsSet = new HashSet<>();
            if (fields != null) {
                fieldsSet.addAll(fields);
            }
            return new FingerprintProcessor(processorTag, fieldsSet, targetField, m, base64encode, concatenateAllFields,
                    ignoreMissing);
        }
    }

    enum Method {
        SHA1, SHA256, MD5, MURMUR3, UUID;

        public static Method parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }
}
