/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment;

import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class AttachmentProcessor extends AbstractProcessor {

    public static final String TYPE = "attachment";

    private static final int NUMBER_OF_CHARS_INDEXED = 100000;

    private final String field;
    private final String targetField;
    private final Set<Property> properties;
    private final int indexedChars;
    private final boolean ignoreMissing;
    private final String indexedCharsField;
    private final String resourceName;

    AttachmentProcessor(String tag, String description, String field, String targetField, Set<Property> properties,
                        int indexedChars, boolean ignoreMissing, String indexedCharsField, String resourceName) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.properties = properties;
        this.indexedChars = indexedChars;
        this.ignoreMissing = ignoreMissing;
        this.indexedCharsField = indexedCharsField;
        this.resourceName = resourceName;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        Map<String, Object> additionalFields = new HashMap<>();

        byte[] input = ingestDocument.getFieldValueAsBytes(field, ignoreMissing);
        String resourceNameInput = null;
        if (resourceName != null) {
            resourceNameInput = ingestDocument.getFieldValue(resourceName, String.class, true);
        }
        if (input == null && ignoreMissing) {
            return ingestDocument;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse.");
        }

        Integer indexedChars = this.indexedChars;

        if (indexedCharsField != null) {
            // If the user provided the number of characters to be extracted as part of the document, we use it
            indexedChars = ingestDocument.getFieldValue(indexedCharsField, Integer.class, true);
            if (indexedChars == null) {
                // If the field does not exist we fall back to the global limit
                indexedChars = this.indexedChars;
            }
        }

        Metadata metadata = new Metadata();
        if (resourceNameInput != null) {
            metadata.set(Metadata.RESOURCE_NAME_KEY, resourceNameInput);
        }
        String parsedContent = "";
        try {
            parsedContent = TikaImpl.parse(input, metadata, indexedChars);
        } catch (ZeroByteFileException e) {
            // tika 1.17 throws an exception when the InputStream has 0 bytes.
            // previously, it did not mind. This is here to preserve that behavior.
        } catch (Exception e) {
            throw new ElasticsearchParseException("Error parsing document in field [{}]", e, field);
        }

        if (properties.contains(Property.CONTENT) && Strings.hasLength(parsedContent)) {
            // somehow tika seems to append a newline at the end automatically, lets remove that again
            additionalFields.put(Property.CONTENT.toLowerCase(), parsedContent.trim());
        }

        if (properties.contains(Property.LANGUAGE) && Strings.hasLength(parsedContent)) {
            // TODO: stop using LanguageIdentifier...
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
            long length;
            if (Strings.hasLength(contentLength)) {
                length = Long.parseLong(contentLength);
            } else {
                length = parsedContent.length();
            }
            additionalFields.put(Property.CONTENT_LENGTH.toLowerCase(), length);
        }

        ingestDocument.setFieldValue(targetField, additionalFields);
        return ingestDocument;
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

    public static final class Factory implements Processor.Factory {

        static final Set<Property> DEFAULT_PROPERTIES = EnumSet.allOf(Property.class);

        @Override
        public AttachmentProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                          String description, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String resourceName = readOptionalStringProperty(TYPE, processorTag, config, "resource_name");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "attachment");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            int indexedChars = readIntProperty(TYPE, processorTag, config, "indexed_chars", NUMBER_OF_CHARS_INDEXED);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            String indexedCharsField = readOptionalStringProperty(TYPE, processorTag, config, "indexed_chars_field");

            final Set<Property> properties;
            if (propertyNames != null) {
                properties = EnumSet.noneOf(Property.class);
                for (String fieldName : propertyNames) {
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

            return new AttachmentProcessor(processorTag, description, field, targetField, properties, indexedChars, ignoreMissing,
                indexedCharsField, resourceName);
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
