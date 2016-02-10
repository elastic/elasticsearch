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

package org.elasticsearch.ingest.attachment;

import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.ingest.core.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readStringProperty;

public final class AttachmentProcessor extends AbstractProcessor {

    public static final String TYPE = "attachment";

    private static final int NUMBER_OF_CHARS_INDEXED = 100000;

    private final String sourceField;
    private final String targetField;
    private final Set<Field> fields;
    private final int indexedChars;

    AttachmentProcessor(String tag, String sourceField, String targetField, Set<Field> fields,
                        int indexedChars) throws IOException {
        super(tag);
        this.sourceField = sourceField;
        this.targetField = targetField;
        this.fields = fields;
        this.indexedChars = indexedChars;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        String base64Input = ingestDocument.getFieldValue(sourceField, String.class);
        Map<String, Object> additionalFields = new HashMap<>();

        try {
            byte[] decodedContent = Base64.decode(base64Input.getBytes(UTF_8));
            Metadata metadata = new Metadata();
            String parsedContent = TikaImpl.parse(decodedContent, metadata, indexedChars);

            if (fields.contains(Field.CONTENT) && Strings.hasLength(parsedContent)) {
                // somehow tika seems to append a newline at the end automatically, lets remove that again
                additionalFields.put(Field.CONTENT.toLowerCase(), parsedContent.trim());
            }

            if (fields.contains(Field.LANGUAGE) && Strings.hasLength(parsedContent)) {
                LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
                String language = identifier.getLanguage();
                additionalFields.put(Field.LANGUAGE.toLowerCase(), language);
            }

            if (fields.contains(Field.DATE)) {
                String createdDate = metadata.get(TikaCoreProperties.CREATED);
                if (createdDate != null) {
                    additionalFields.put(Field.DATE.toLowerCase(), createdDate);
                }
            }

            if (fields.contains(Field.TITLE)) {
                String title = metadata.get(TikaCoreProperties.TITLE);
                if (Strings.hasLength(title)) {
                    additionalFields.put(Field.TITLE.toLowerCase(), title);
                }
            }

            if (fields.contains(Field.AUTHOR)) {
                String author = metadata.get("Author");
                if (Strings.hasLength(author)) {
                    additionalFields.put(Field.AUTHOR.toLowerCase(), author);
                }
            }

            if (fields.contains(Field.KEYWORDS)) {
                String keywords = metadata.get("Keywords");
                if (Strings.hasLength(keywords)) {
                    additionalFields.put(Field.KEYWORDS.toLowerCase(), keywords);
                }
            }

            if (fields.contains(Field.CONTENT_TYPE)) {
                String contentType = metadata.get(Metadata.CONTENT_TYPE);
                if (Strings.hasLength(contentType)) {
                    additionalFields.put(Field.CONTENT_TYPE.toLowerCase(), contentType);
                }
            }

            if (fields.contains(Field.CONTENT_LENGTH)) {
                String contentLength = metadata.get(Metadata.CONTENT_LENGTH);
                String length = Strings.hasLength(contentLength) ? contentLength : String.valueOf(parsedContent.length());
                additionalFields.put(Field.CONTENT_LENGTH.toLowerCase(), length);
            }
        } catch (Throwable e) {
            throw new ElasticsearchParseException("Error parsing document in field [{}]", e, sourceField);
        }

        ingestDocument.setFieldValue(targetField, additionalFields);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getSourceField() {
        return sourceField;
    }

    String getTargetField() {
        return targetField;
    }

    Set<Field> getFields() {
        return fields;
    }

    int getIndexedChars() {
        return indexedChars;
    }

    public static final class Factory extends AbstractProcessorFactory<AttachmentProcessor> {

        static final Set<Field> DEFAULT_FIELDS = EnumSet.allOf(Field.class);

        @Override
        public AttachmentProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String sourceField = readStringProperty(TYPE, processorTag, config, "source_field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "attachment");
            List<String> fieldNames = readOptionalList(TYPE, processorTag, config, "fields");
            int indexedChars = readIntProperty(TYPE, processorTag, config, "indexed_chars", NUMBER_OF_CHARS_INDEXED);

            final Set<Field> fields;
            if (fieldNames != null) {
                fields = EnumSet.noneOf(Field.class);
                for (String fieldName : fieldNames) {
                    try {
                        fields.add(Field.parse(fieldName));
                    } catch (Exception e) {
                        throw newConfigurationException(TYPE, processorTag, "fields", "illegal field option [" +
                            fieldName + "]. valid values are " + Arrays.toString(Field.values()));
                    }
                }
            } else {
                fields = DEFAULT_FIELDS;
            }

            return new AttachmentProcessor(processorTag, sourceField, targetField, fields, indexedChars);
        }
    }

    public enum Field {

        CONTENT,
        TITLE,
        AUTHOR,
        KEYWORDS,
        DATE,
        CONTENT_TYPE,
        CONTENT_LENGTH,
        LANGUAGE;

        public static Field parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        public String toLowerCase() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
