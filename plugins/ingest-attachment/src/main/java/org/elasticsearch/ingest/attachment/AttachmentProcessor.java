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

import static org.elasticsearch.ingest.core.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readStringProperty;

public final class AttachmentProcessor extends AbstractProcessor {

    public static final String TYPE = "attachment";

    private static final int NUMBER_OF_CHARS_INDEXED = 100000;

    private final String field;
    private final String targetField;
    private final Set<Property> properties;
    private final int indexedChars;

    AttachmentProcessor(String tag, String field, String targetField, Set<Property> properties,
                        int indexedChars) throws IOException {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.properties = properties;
        this.indexedChars = indexedChars;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        Map<String, Object> additionalFields = new HashMap<>();

        try {
            Metadata metadata = new Metadata();
            byte[] input = ingestDocument.getFieldValueAsBytes(field);
            String parsedContent = TikaImpl.parse(input, metadata, indexedChars);

            if (properties.contains(Property.CONTENT) && Strings.hasLength(parsedContent)) {
                // somehow tika seems to append a newline at the end automatically, lets remove that again
                additionalFields.put(Property.CONTENT.toLowerCase(), parsedContent.trim());
            }

            if (properties.contains(Property.LANGUAGE) && Strings.hasLength(parsedContent)) {
                LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
                String language = identifier.getLanguage();
                additionalFields.put(Property.LANGUAGE.toLowerCase(), language);
            }

            if (properties.contains(Property.DATE)) {
                String createdDate = metadata.get(TikaCoreProperties.CREATED);
                if (createdDate != null) {
                    additionalFields.put(Property.DATE.toLowerCase(), createdDate);
                }
            }

            if (properties.contains(Property.TITLE)) {
                String title = metadata.get(TikaCoreProperties.TITLE);
                if (Strings.hasLength(title)) {
                    additionalFields.put(Property.TITLE.toLowerCase(), title);
                }
            }

            if (properties.contains(Property.AUTHOR)) {
                String author = metadata.get("Author");
                if (Strings.hasLength(author)) {
                    additionalFields.put(Property.AUTHOR.toLowerCase(), author);
                }
            }

            if (properties.contains(Property.KEYWORDS)) {
                String keywords = metadata.get("Keywords");
                if (Strings.hasLength(keywords)) {
                    additionalFields.put(Property.KEYWORDS.toLowerCase(), keywords);
                }
            }

            if (properties.contains(Property.CONTENT_TYPE)) {
                String contentType = metadata.get(Metadata.CONTENT_TYPE);
                if (Strings.hasLength(contentType)) {
                    additionalFields.put(Property.CONTENT_TYPE.toLowerCase(), contentType);
                }
            }

            if (properties.contains(Property.CONTENT_LENGTH)) {
                String contentLength = metadata.get(Metadata.CONTENT_LENGTH);
                String length = Strings.hasLength(contentLength) ? contentLength : String.valueOf(parsedContent.length());
                additionalFields.put(Property.CONTENT_LENGTH.toLowerCase(), length);
            }
        } catch (Throwable e) {
            throw new ElasticsearchParseException("Error parsing document in field [{}]", e, field);
        }

        ingestDocument.setFieldValue(targetField, additionalFields);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    Set<Property> getProperties() {
        return properties;
    }

    int getIndexedChars() {
        return indexedChars;
    }

    public static final class Factory extends AbstractProcessorFactory<AttachmentProcessor> {

        static final Set<Property> DEFAULT_PROPERTIES = EnumSet.allOf(Property.class);

        @Override
        public AttachmentProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "attachment");
            List<String> properyNames = readOptionalList(TYPE, processorTag, config, "properties");
            int indexedChars = readIntProperty(TYPE, processorTag, config, "indexed_chars", NUMBER_OF_CHARS_INDEXED);

            final Set<Property> properties;
            if (properyNames != null) {
                properties = EnumSet.noneOf(Property.class);
                for (String fieldName : properyNames) {
                    try {
                        properties.add(Property.parse(fieldName));
                    } catch (Exception e) {
                        throw newConfigurationException(TYPE, processorTag, "properties", "illegal field option [" +
                            fieldName + "]. valid values are " + Arrays.toString(Property.values()));
                    }
                }
            } else {
                properties = DEFAULT_PROPERTIES;
            }

            return new AttachmentProcessor(processorTag, field, targetField, properties, indexedChars);
        }
    }

    enum Property {

        CONTENT,
        TITLE,
        AUTHOR,
        KEYWORDS,
        DATE,
        CONTENT_TYPE,
        CONTENT_LENGTH,
        LANGUAGE;

        public static Property parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        public String toLowerCase() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
