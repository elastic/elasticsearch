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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.UpdateForV10;
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

/**
 * Ingest processor that extracts text content and metadata from binary documents using Apache Tika.
 * <p>
 * This processor parses documents in various formats (PDF, Microsoft Office, HTML, etc.) and extracts
 * information including content, title, author, keywords, dates, and other metadata. The extracted
 * data is added to a specified target field in the ingest document.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic usage in an ingest pipeline:
 * {
 *   "attachment": {
 *     "field": "data",
 *     "target_field": "attachment",
 *     "indexed_chars": 100000,
 *     "properties": ["content", "title", "author"],
 *     "ignore_missing": false,
 *     "remove_binary": true
 *   }
 * }
 * }</pre>
 */
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

    /**
     * Executes the attachment processor on an ingest document.
     * <p>
     * This method extracts the binary content from the specified source field, parses it using
     * Apache Tika, and populates the target field with extracted content and metadata. The method
     * handles various document formats and can be configured to extract specific properties,
     * limit indexed characters, and optionally remove the binary field after processing.
     * </p>
     *
     * @param ingestDocument the document to process
     * @return the modified ingest document with extracted attachment data
     * @throws IllegalArgumentException if the source field is null and ignore_missing is false
     * @throws ElasticsearchParseException if an error occurs while parsing the document
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Executed automatically as part of an ingest pipeline
     * IngestDocument doc = new IngestDocument(...);
     * doc.setFieldValue("data", base64EncodedPdfBytes);
     * IngestDocument result = processor.execute(doc);
     * // result now contains extracted data in the target field
     * Map<String, Object> attachment = result.getFieldValue("attachment", Map.class);
     * String content = (String) attachment.get("content");
     * }</pre>
     */
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
    private void addAdditionalField(Map<String, Object> additionalFields, Property property, String value) {
        if (properties.contains(property) && Strings.hasLength(value)) {
            additionalFields.put(property.toLowerCase(), value);
        }
    }

    /**
     * Returns the processor type name.
     *
     * @return the string "attachment" identifying this processor type
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String type = processor.getType();
     * // Returns: "attachment"
     * }</pre>
     */
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

    /**
     * Factory for creating AttachmentProcessor instances.
     * <p>
     * This factory reads processor configuration from pipeline definitions and creates
     * configured attachment processor instances with appropriate settings for field extraction,
     * property selection, character limits, and binary removal options.
     * </p>
     */
    public static final class Factory implements Processor.Factory {

        static final Set<Property> DEFAULT_PROPERTIES = EnumSet.allOf(Property.class);

        /**
         * Creates an AttachmentProcessor from the provided configuration.
         * <p>
         * This method parses the processor configuration including source field, target field,
         * properties to extract, character limits, and other options. It validates the configuration
         * and returns a configured processor instance.
         * </p>
         *
         * @param registry the processor factory registry (unused)
         * @param processorTag the processor tag for error reporting
         * @param description the processor description
         * @param config the processor configuration map
         * @param projectId the project identifier
         * @return a configured AttachmentProcessor instance
         * @throws ElasticsearchParseException if the configuration is invalid
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Configuration in pipeline definition:
         * Map<String, Object> config = Map.of(
         *     "field", "data",
         *     "target_field", "attachment",
         *     "indexed_chars", 50000,
         *     "properties", List.of("content", "title", "author"),
         *     "ignore_missing", true,
         *     "remove_binary", true
         * );
         * AttachmentProcessor processor = factory.create(registry, "tag1", "desc", config, projectId);
         * }</pre>
         */
        @Override
        public AttachmentProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String resourceName = readOptionalStringProperty(TYPE, processorTag, config, "resource_name");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "attachment");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            int indexedChars = readIntProperty(TYPE, processorTag, config, "indexed_chars", NUMBER_OF_CHARS_INDEXED);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            String indexedCharsField = readOptionalStringProperty(TYPE, processorTag, config, "indexed_chars_field");
            @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
            // Revisit whether we want to update the [remove_binary] default to be 'true' - would need to find a way to do this safely
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

    /**
     * Enumeration of document properties that can be extracted by the attachment processor.
     * <p>
     * These properties represent metadata fields that Apache Tika can extract from various
     * document formats. Each property corresponds to a specific metadata field such as content,
     * title, author, dates, geolocation, and more.
     * </p>
     */
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

        /**
         * Parses a property name string into a Property enum value.
         * <p>
         * The parsing is case-insensitive, converting the input to uppercase before matching.
         * </p>
         *
         * @param value the property name to parse
         * @return the corresponding Property enum value
         * @throws IllegalArgumentException if the value does not match any property
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Property prop1 = Property.parse("content");  // Returns CONTENT
         * Property prop2 = Property.parse("TITLE");    // Returns TITLE
         * Property prop3 = Property.parse("Author");   // Returns AUTHOR
         * }</pre>
         */
        public static Property parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        /**
         * Returns the lowercase string representation of this property.
         * <p>
         * This method is used when adding extracted metadata to the document, ensuring
         * consistent lowercase field names in the output.
         * </p>
         *
         * @return the property name in lowercase
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String fieldName = Property.CONTENT.toLowerCase();  // Returns "content"
         * String titleField = Property.TITLE.toLowerCase();   // Returns "title"
         * }</pre>
         */
        public String toLowerCase() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
