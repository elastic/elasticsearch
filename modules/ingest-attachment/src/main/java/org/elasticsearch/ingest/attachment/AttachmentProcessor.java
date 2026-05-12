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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.monitor.jvm.JvmInfo;

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

    /**
     * When set to a value, caps the raw in-memory size of the attachment source {@code field} for any {@code attachment} processor that
     * does not set {@code max_field_bytes}, in a similar way that {@code max_field_bytes} caps. Absolute values apply directly;
     * ratio or percentage values are resolved against the JVM's maximum heap size for the node. Note that attachment sizes are
     * limited to an upper bound of an integer size. -1 means no limit.
     */
    public static final Setting<RelativeByteSizeValue> MAX_FIELD_SIZE_SETTING = new Setting<>(
        "ingest.attachment.max_field_size",
        "-1",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "ingest.attachment.max_field_size"),
        Setting.Property.NodeScope
    );
    /**
     * Optional custom message suffix used when {@link #MAX_FIELD_SIZE_SETTING} rejects an attachment.
     * If set, replaces the default detailed message suffix (which includes the configured limit and setting value).
     */
    public static final Setting<String> MAX_FIELD_SIZE_MESSAGE_SUFFIX_SETTING = Setting.simpleString(
        "ingest.attachment.max_field_size_message_suffix",
        "",
        Setting.Property.NodeScope
    );

    private final String field;
    private final String targetField;
    private final Set<Property> properties;
    private final int indexedChars;
    private final boolean ignoreMissing;
    private final boolean removeBinary;
    private final String indexedCharsField;
    private final String resourceName;
    // Explicit per-processor cap on raw attachment field size in bytes, or -1 to fall back to the node-level cap.
    private final int maxFieldBytesFromProcessor;
    // Node-level cap from {@link #MAX_FIELD_SIZE_SETTING}, or -1 if not applicable.
    private final RelativeByteSizeValue maxFieldSizeFromNode;
    private final long maxFieldSizeFromNodeBytes;
    private final String maxFieldSizeExceededMessage;

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
        boolean removeBinary,
        int maxFieldBytesFromProcessor,
        RelativeByteSizeValue maxFieldSizeFromNode,
        String maxFieldSizeExceededMessage
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
        this.maxFieldBytesFromProcessor = maxFieldBytesFromProcessor;
        this.maxFieldSizeFromNode = maxFieldSizeFromNode;
        this.maxFieldSizeFromNodeBytes = resolveMaxFieldSizeFromNode(maxFieldSizeFromNode);
        this.maxFieldSizeExceededMessage = maxFieldSizeExceededMessage;
    }

    /**
     * Resolves {@link #MAX_FIELD_SIZE_SETTING} to an absolute byte cap, or -1 if not applicable.
     */
    private static long resolveMaxFieldSizeFromNode(RelativeByteSizeValue maxFieldSizeFromNode) {
        if (maxFieldSizeFromNode.isAbsolute()) {
            return maxFieldSizeFromNode.getAbsolute().getBytes();
        }
        long heapMaxBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        if (heapMaxBytes <= 0) {
            return -1L;
        }
        return maxFieldSizeFromNode.calculateValue(ByteSizeValue.ofBytes(heapMaxBytes), null).getBytes();
    }

    private void checkMaxAttachmentFieldSize(final int fieldSizeBytes) {
        if (maxFieldBytesFromProcessor >= 0) {
            if (fieldSizeBytes > maxFieldBytesFromProcessor) {
                throw new ElasticsearchParseException(
                    "field [{}] has an attachment field size of [{}] bytes exceeding the maximum allowed processor size of [{}] bytes",
                    field,
                    fieldSizeBytes,
                    maxFieldBytesFromProcessor
                );
            }
        } else {
            if (maxFieldSizeFromNodeBytes >= 0 && fieldSizeBytes > maxFieldSizeFromNodeBytes) {
                if (Strings.hasLength(maxFieldSizeExceededMessage)) {
                    throw new ElasticsearchParseException(
                        "field [{}] has an attachment field size of [{}] bytes exceeding the maximum allowed input size {}",
                        field,
                        fieldSizeBytes,
                        maxFieldSizeExceededMessage
                    );
                }
                throw new ElasticsearchParseException(
                    "field [{}] has an attachment field size of [{}] bytes exceeding the maximum allowed input size of [{}] bytes "
                        + "due to setting [{}={}]",
                    field,
                    fieldSizeBytes,
                    maxFieldSizeFromNodeBytes,
                    MAX_FIELD_SIZE_SETTING.getKey(),
                    maxFieldSizeFromNode.getStringRep()
                );
            }
        }
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    // For tests only
    boolean isRemoveBinary() {
        return removeBinary;
    }

    // For tests only
    int getMaxFieldBytesFromProcessor() {
        return maxFieldBytesFromProcessor;
    }

    // For tests only
    RelativeByteSizeValue getMaxFieldSizeFromNode() {
        return maxFieldSizeFromNode;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        Map<String, Object> additionalFields = new HashMap<>();

        Object fieldValue = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);
        String resourceNameInput = null;
        if (resourceName != null) {
            resourceNameInput = ingestDocument.getFieldValue(resourceName, String.class, true);
        }
        if (fieldValue == null && ignoreMissing) {
            return ingestDocument;
        } else if (fieldValue == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse.");
        }
        checkMaxAttachmentFieldSize(ingestDocument.getFieldValueRawBytesLength(field, fieldValue));
        byte[] input = ingestDocument.getFieldValueAsBytes(field, fieldValue);

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

        private final RelativeByteSizeValue maxFieldSizeFromNode;
        private final String maxFieldSizeExceededMessage;

        public Factory(Settings nodeSettings) {
            this.maxFieldSizeFromNode = MAX_FIELD_SIZE_SETTING.get(nodeSettings);
            this.maxFieldSizeExceededMessage = MAX_FIELD_SIZE_MESSAGE_SUFFIX_SETTING.get(nodeSettings);
        }

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
            @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED)
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

            int maxFieldBytesFromProcessor = readIntProperty(TYPE, processorTag, config, "max_field_bytes", -1);
            if (maxFieldBytesFromProcessor < -1L) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "max_field_bytes",
                    "illegal value [" + maxFieldBytesFromProcessor + "]. must be -1 or higher."
                );
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
                removeBinary,
                maxFieldBytesFromProcessor,
                maxFieldSizeFromNode,
                maxFieldSizeExceededMessage
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
