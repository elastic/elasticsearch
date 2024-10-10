/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.langdetect.tika.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.UpdateForV9;
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
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class AttachmentProcessor extends AbstractProcessor {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(AttachmentProcessor.class);
    private static final int NUMBER_OF_CHARS_INDEXED = 100000;

    public static final String TYPE = "attachment";

    private final String field;
    private final String targetField;
    private final Set<Property> properties;
    private final int indexedChars;
    private final boolean ignoreMissing;
    private final boolean removeBinary;
    private final String indexedCharsField;
    private final String resourceName;

    AttachmentProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        Set<Property> properties,
        int indexedChars,
        boolean ignoreMissing,
        String indexedCharsField,
        String resourceName,
        boolean removeBinary
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.properties = properties;
        this.indexedChars = indexedChars;
        this.ignoreMissing = ignoreMissing;
        this.indexedCharsField = indexedCharsField;
        this.resourceName = resourceName;
        this.removeBinary = removeBinary;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    // For tests only
    boolean isRemoveBinary() {
        return removeBinary;
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

        Integer indexedCharsValue = this.indexedChars;

        if (indexedCharsField != null) {
            // If the user provided the number of characters to be extracted as part of the document, we use it
            indexedCharsValue = ingestDocument.getFieldValue(indexedCharsField, Integer.class, true);
            if (indexedCharsValue == null) {
                // If the field does not exist we fall back to the global limit
                indexedCharsValue = this.indexedChars;
            }
        }

        Metadata metadata = new Metadata();
        if (resourceNameInput != null) {
            metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, resourceNameInput);
        }
        String parsedContent = "";
        try {
            parsedContent = TikaImpl.parse(input, metadata, indexedCharsValue);
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

        addAdditionalField(additionalFields, Property.DATE, metadata.get(TikaCoreProperties.CREATED));
        addAdditionalField(additionalFields, Property.TITLE, metadata.get(TikaCoreProperties.TITLE));
        // These two were supposedly removed in tika 2, but some parsers seem to still generate them:
        addAdditionalField(additionalFields, Property.AUTHOR, metadata.get("Author"));
        addAdditionalField(additionalFields, Property.KEYWORDS, metadata.get("Keywords"));
        addAdditionalField(additionalFields, Property.KEYWORDS, metadata.get(TikaCoreProperties.SUBJECT));
        addAdditionalField(additionalFields, Property.CONTENT_TYPE, metadata.get(Metadata.CONTENT_TYPE));

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

        addAdditionalField(additionalFields, Property.AUTHOR, metadata.get(TikaCoreProperties.CREATOR));
        addAdditionalField(additionalFields, Property.KEYWORDS, metadata.get(Office.KEYWORDS));

        addAdditionalField(additionalFields, Property.MODIFIED, metadata.get(TikaCoreProperties.MODIFIED));
        addAdditionalField(additionalFields, Property.FORMAT, metadata.get(TikaCoreProperties.FORMAT));
        addAdditionalField(additionalFields, Property.IDENTIFIER, metadata.get(TikaCoreProperties.IDENTIFIER));
        addAdditionalField(additionalFields, Property.CONTRIBUTOR, metadata.get(TikaCoreProperties.CONTRIBUTOR));
        addAdditionalField(additionalFields, Property.COVERAGE, metadata.get(TikaCoreProperties.COVERAGE));
        addAdditionalField(additionalFields, Property.MODIFIER, metadata.get(TikaCoreProperties.MODIFIER));
        addAdditionalField(additionalFields, Property.CREATOR_TOOL, metadata.get(TikaCoreProperties.CREATOR_TOOL));
        addAdditionalField(additionalFields, Property.PUBLISHER, metadata.get(TikaCoreProperties.PUBLISHER));
        addAdditionalField(additionalFields, Property.RELATION, metadata.get(TikaCoreProperties.RELATION));
        addAdditionalField(additionalFields, Property.RIGHTS, metadata.get(TikaCoreProperties.RIGHTS));
        addAdditionalField(additionalFields, Property.SOURCE, metadata.get(TikaCoreProperties.SOURCE));
        addAdditionalField(additionalFields, Property.TYPE, metadata.get(TikaCoreProperties.TYPE));
        addAdditionalField(additionalFields, Property.DESCRIPTION, metadata.get(TikaCoreProperties.DESCRIPTION));
        addAdditionalField(additionalFields, Property.PRINT_DATE, metadata.get(TikaCoreProperties.PRINT_DATE));
        addAdditionalField(additionalFields, Property.METADATA_DATE, metadata.get(TikaCoreProperties.METADATA_DATE));
        addAdditionalField(additionalFields, Property.LATITUDE, metadata.get(TikaCoreProperties.LATITUDE));
        addAdditionalField(additionalFields, Property.LONGITUDE, metadata.get(TikaCoreProperties.LONGITUDE));
        addAdditionalField(additionalFields, Property.ALTITUDE, metadata.get(TikaCoreProperties.ALTITUDE));
        addAdditionalField(additionalFields, Property.RATING, metadata.get(TikaCoreProperties.RATING));
        addAdditionalField(additionalFields, Property.COMMENTS, metadata.get(TikaCoreProperties.COMMENTS));

        ingestDocument.setFieldValue(targetField, additionalFields);

        if (removeBinary) {
            ingestDocument.removeField(field);
        }
        return ingestDocument;
    }

    /**
     * Add an additional field if not null or empty
     * @param additionalFields  additional fields
     * @param property          property to add
     * @param value             value to add
     */
    private <T> void addAdditionalField(Map<String, Object> additionalFields, Property property, String value) {
        if (properties.contains(property) && Strings.hasLength(value)) {
            additionalFields.put(property.toLowerCase(), value);
        }
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
        public AttachmentProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String resourceName = readOptionalStringProperty(TYPE, processorTag, config, "resource_name");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "attachment");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            int indexedChars = readIntProperty(TYPE, processorTag, config, "indexed_chars", NUMBER_OF_CHARS_INDEXED);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            String indexedCharsField = readOptionalStringProperty(TYPE, processorTag, config, "indexed_chars_field");
            @UpdateForV9(owner = UpdateForV9.Owner.DATA_MANAGEMENT)
            // update the [remove_binary] default to be 'true' assuming enough time has passed. Deprecated in September 2022.
            Boolean removeBinary = readOptionalBooleanProperty(TYPE, processorTag, config, "remove_binary");
            if (removeBinary == null) {
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.PARSING,
                    "attachment-remove-binary",
                    "The default [remove_binary] value of 'false' is deprecated and will be "
                        + "set to 'true' in a future release. Set [remove_binary] explicitly to "
                        + "'true' or 'false' to ensure no behavior change."
                );
                removeBinary = false;
            }

            final Set<Property> properties;
            if (propertyNames != null) {
                properties = EnumSet.noneOf(Property.class);
                for (String fieldName : propertyNames) {
                    try {
                        properties.add(Property.parse(fieldName));
                    } catch (Exception e) {
                        throw newConfigurationException(
                            TYPE,
                            processorTag,
                            "properties",
                            "illegal field option [" + fieldName + "]. valid values are " + Arrays.toString(Property.values())
                        );
                    }
                }
            } else {
                properties = DEFAULT_PROPERTIES;
            }

            return new AttachmentProcessor(
                processorTag,
                description,
                field,
                targetField,
                properties,
                indexedChars,
                ignoreMissing,
                indexedCharsField,
                resourceName,
                removeBinary
            );
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
        LANGUAGE,
        MODIFIED,
        FORMAT,
        IDENTIFIER,
        CONTRIBUTOR,
        COVERAGE,
        MODIFIER,
        CREATOR_TOOL,
        PUBLISHER,
        RELATION,
        RIGHTS,
        SOURCE,
        TYPE,
        DESCRIPTION,
        PRINT_DATE,
        METADATA_DATE,
        LATITUDE,
        LONGITUDE,
        ALTITUDE,
        RATING,
        COMMENTS;

        public static Property parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        public String toLowerCase() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
