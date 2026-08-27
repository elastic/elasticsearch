/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.lucene.util.SetOnce;
import org.apache.tika.langdetect.charsoup.CharSoupLanguageDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

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

    /**
     * Per-thread language detector. {@link LanguageDetector} is not thread-safe (its
     * {@code detect(CharSequence)} method mutates internal state), so each ingest thread
     * owns a separate instance. Model loading is cheap because {@link CharSoupLanguageDetector}
     * holds its language profiles in a static field shared across all instances.
     */
    private static final ThreadLocal<LanguageDetector> LANGUAGE_DETECTOR = ThreadLocal.withInitial(() -> {
        try {
            return new CharSoupLanguageDetector().loadModels();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Tika language-detection models", e);
        }
    });

    /**
     * Maps ISO 639-2 three-letter codes (e.g. {@code "eng"}) to ISO 639-1 two-letter codes
     * (e.g. {@code "en"}). Built once from the JVM's known locales so no external data file
     * is needed. Languages that have no ISO 639-1 code are absent from the map; callers
     * should fall back to the raw code in that case.
     */
    private static final Map<String, String> ISO639_3_TO_1;

    static {
        Map<String, String> map = new HashMap<>();
        for (String lang : Locale.getISOLanguages()) {
            @SuppressWarnings("deprecation")
            Locale locale = new Locale(lang);
            String iso3 = locale.getISO3Language();
            if (iso3 != null && iso3.isEmpty() == false) {
                map.put(iso3, lang);
            }
        }
        ISO639_3_TO_1 = Collections.unmodifiableMap(map);
    }

    public static final String TYPE = "attachment";

    /**
     * When set to a value, caps the raw in-memory size of the attachment source {@code field} for every {@code attachment} processor
     * on the node, regardless of {@code max_field_bytes}. If both are set, the effective limit is the stricter of the two. Absolute values
     * apply directly; ratio or percentage values are resolved against the JVM's maximum heap size for the node. Note that attachment
     * sizes are limited to an upper bound of an integer size. -1 means no node-level limit.
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
    // Per-processor cap on raw attachment field size in bytes, or -1 for no per-processor cap (node cap still applies when set).
    private final int maxFieldBytesFromProcessor;
    // Node-level cap from {@link #MAX_FIELD_SIZE_SETTING}, or -1 if not applicable.
    private final RelativeByteSizeValue maxFieldSizeFromNode;
    private final long maxFieldSizeFromNodeBytes;
    private final String maxFieldSizeExceededMessage;
    private final SetOnce<AttachmentIngestMetrics> attachmentMetrics;
    private final ExtractionBackend backend;

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
        String maxFieldSizeExceededMessage,
        SetOnce<AttachmentIngestMetrics> attachmentMetrics,
        ExtractionBackend backend
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
        this.attachmentMetrics = attachmentMetrics;
        this.backend = backend;
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
        if (maxFieldBytesFromProcessor >= 0 && fieldSizeBytes > maxFieldBytesFromProcessor) {
            throw new ElasticsearchParseException(
                "field [{}] has an attachment field size of [{}] bytes exceeding the maximum allowed processor size of [{}] bytes",
                field,
                fieldSizeBytes,
                maxFieldBytesFromProcessor
            );
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
    public boolean isAsync() {
        return backend instanceof TikaServerExtractionBackend;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        assert isAsync() == false : "sync execute called on async processor";

        Object fieldValue = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);
        String resourceNameInput = resolveResourceName(ingestDocument);
        if (fieldValue == null && ignoreMissing) {
            return ingestDocument;
        } else if (fieldValue == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse.");
        }

        final int rawBytes = ingestDocument.getFieldValueRawBytesLength(field, fieldValue);
        if (attachmentMetrics.get() != null) {
            attachmentMetrics.get().recordRawBytesReceived(rawBytes);
        }
        checkMaxAttachmentFieldSize(rawBytes);
        byte[] input = ingestDocument.getFieldValueAsBytes(field, fieldValue);
        int indexedCharsValue = computeIndexedChars(ingestDocument);

        // LocalExtractionBackend always fires the listener synchronously
        final ExtractionResult[] resultRef = new ExtractionResult[1];
        final Exception[] exceptionRef = new Exception[1];
        backend.extract(
            input,
            resourceNameInput,
            indexedCharsValue,
            ActionListener.wrap(result -> resultRef[0] = result, ex -> exceptionRef[0] = ex)
        );

        if (exceptionRef[0] != null) {
            throw new ElasticsearchParseException("Error parsing document in field [{}]", exceptionRef[0], field);
        }
        assert resultRef[0] != null : "LocalExtractionBackend must call listener synchronously";

        applyResult(ingestDocument, resultRef[0]);

        if (removeBinary) {
            ingestDocument.removeField(field);
        }
        if (attachmentMetrics.get() != null) {
            attachmentMetrics.get().recordRawBytesProcessed(rawBytes);
        }
        return ingestDocument;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        assert isAsync() : "async execute called on sync processor";

        Object fieldValue = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);
        String resourceNameInput = resolveResourceName(ingestDocument);
        if (fieldValue == null && ignoreMissing) {
            handler.accept(ingestDocument, null);
            return;
        } else if (fieldValue == null) {
            handler.accept(null, new IllegalArgumentException("field [" + field + "] is null, cannot parse."));
            return;
        }

        final int rawBytes = ingestDocument.getFieldValueRawBytesLength(field, fieldValue);
        if (attachmentMetrics.get() != null) {
            attachmentMetrics.get().recordRawBytesReceived(rawBytes);
        }
        try {
            checkMaxAttachmentFieldSize(rawBytes);
        } catch (Exception e) {
            handler.accept(null, e);
            return;
        }
        byte[] input = ingestDocument.getFieldValueAsBytes(field, fieldValue);
        int indexedCharsValue = computeIndexedChars(ingestDocument);

        backend.extract(input, resourceNameInput, indexedCharsValue, ActionListener.wrap(result -> {
            try {
                applyResult(ingestDocument, result);
                if (removeBinary) {
                    ingestDocument.removeField(field);
                }
                if (attachmentMetrics.get() != null) {
                    attachmentMetrics.get().recordRawBytesProcessed(rawBytes);
                }
                handler.accept(ingestDocument, null);
            } catch (Exception e) {
                handler.accept(null, e);
            }
        }, e -> handler.accept(null, new ElasticsearchParseException("Error parsing document in field [{}]", e, field))));
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

    private String resolveResourceName(IngestDocument ingestDocument) {
        if (resourceName == null) {
            return null;
        }
        return ingestDocument.getFieldValue(resourceName, String.class, true);
    }

    private int computeIndexedChars(IngestDocument ingestDocument) {
        if (indexedCharsField != null) {
            Integer fieldValue = ingestDocument.getFieldValue(indexedCharsField, Integer.class, true);
            if (fieldValue != null) {
                return fieldValue;
            }
        }
        return indexedChars;
    }

    /**
     * Applies an {@link ExtractionResult} to an ingest document, populating the target field map
     * from the extracted content and metadata.
     */
    private void applyResult(IngestDocument ingestDocument, ExtractionResult result) {
        Map<String, Object> additionalFields = new HashMap<>();
        String parsedContent = result.content();
        Map<String, String> meta = result.metadata();

        if (properties.contains(Property.CONTENT) && Strings.hasLength(parsedContent)) {
            // tika appends a newline at the end automatically; remove it
            additionalFields.put(Property.CONTENT.toLowerCase(), parsedContent.trim());
        }

        if (properties.contains(Property.LANGUAGE)) {
            // Prefer server-side language detection (e.g. CharSoupMetadataFilter on tika-server)
            // stored in TikaCoreProperties.TIKA_DETECTED_LANGUAGE. Fall back to in-process
            // detection only for the local backend (isAsync() == false).
            String detectedLanguage = meta.get(TikaCoreProperties.TIKA_DETECTED_LANGUAGE.getName());
            if (detectedLanguage == null && isAsync() == false && Strings.hasLength(parsedContent)) {
                String raw = LANGUAGE_DETECTOR.get().detect(parsedContent).getLanguage();
                // CharSoupLanguageDetector returns ISO 639-2 3-letter codes; normalise to ISO 639-1
                detectedLanguage = raw != null ? ISO639_3_TO_1.getOrDefault(raw, raw) : null;
            }
            if (detectedLanguage != null) {
                additionalFields.put(Property.LANGUAGE.toLowerCase(), detectedLanguage);
            }
        }

        addAdditionalField(additionalFields, Property.DATE, meta.get(TikaCoreProperties.CREATED.getName()));
        addAdditionalField(additionalFields, Property.TITLE, meta.get(TikaCoreProperties.TITLE.getName()));
        // These two were supposedly removed in tika 2, but some parsers seem to still generate them:
        addAdditionalField(additionalFields, Property.AUTHOR, meta.get("Author"));
        addAdditionalField(additionalFields, Property.KEYWORDS, meta.get("Keywords"));
        addAdditionalField(additionalFields, Property.KEYWORDS, meta.get(TikaCoreProperties.SUBJECT.getName()));
        addAdditionalField(additionalFields, Property.CONTENT_TYPE, meta.get(Metadata.CONTENT_TYPE));

        if (properties.contains(Property.CONTENT_LENGTH)) {
            String contentLength = meta.get(Metadata.CONTENT_LENGTH);
            long length;
            if (Strings.hasLength(contentLength)) {
                length = Long.parseLong(contentLength);
            } else {
                length = parsedContent.length();
            }
            additionalFields.put(Property.CONTENT_LENGTH.toLowerCase(), length);
        }

        addAdditionalField(additionalFields, Property.AUTHOR, meta.get(TikaCoreProperties.CREATOR.getName()));
        addAdditionalField(additionalFields, Property.KEYWORDS, meta.get(Office.KEYWORDS.getName()));

        addAdditionalField(additionalFields, Property.MODIFIED, meta.get(TikaCoreProperties.MODIFIED.getName()));
        addAdditionalField(additionalFields, Property.FORMAT, meta.get(TikaCoreProperties.FORMAT.getName()));
        addAdditionalField(additionalFields, Property.IDENTIFIER, meta.get(TikaCoreProperties.IDENTIFIER.getName()));
        addAdditionalField(additionalFields, Property.CONTRIBUTOR, meta.get(TikaCoreProperties.CONTRIBUTOR.getName()));
        addAdditionalField(additionalFields, Property.COVERAGE, meta.get(TikaCoreProperties.COVERAGE.getName()));
        addAdditionalField(additionalFields, Property.MODIFIER, meta.get(TikaCoreProperties.MODIFIER.getName()));
        addAdditionalField(additionalFields, Property.CREATOR_TOOL, meta.get(TikaCoreProperties.CREATOR_TOOL.getName()));
        addAdditionalField(additionalFields, Property.PUBLISHER, meta.get(TikaCoreProperties.PUBLISHER.getName()));
        addAdditionalField(additionalFields, Property.RELATION, meta.get(TikaCoreProperties.RELATION.getName()));
        addAdditionalField(additionalFields, Property.RIGHTS, meta.get(TikaCoreProperties.RIGHTS.getName()));
        addAdditionalField(additionalFields, Property.SOURCE, meta.get(TikaCoreProperties.SOURCE.getName()));
        addAdditionalField(additionalFields, Property.TYPE, meta.get(TikaCoreProperties.TYPE.getName()));
        addAdditionalField(additionalFields, Property.DESCRIPTION, meta.get(TikaCoreProperties.DESCRIPTION.getName()));
        addAdditionalField(additionalFields, Property.PRINT_DATE, meta.get(TikaCoreProperties.PRINT_DATE.getName()));
        addAdditionalField(additionalFields, Property.METADATA_DATE, meta.get(TikaCoreProperties.METADATA_DATE.getName()));
        addAdditionalField(additionalFields, Property.LATITUDE, meta.get(TikaCoreProperties.LATITUDE.getName()));
        addAdditionalField(additionalFields, Property.LONGITUDE, meta.get(TikaCoreProperties.LONGITUDE.getName()));
        addAdditionalField(additionalFields, Property.ALTITUDE, meta.get(TikaCoreProperties.ALTITUDE.getName()));
        addAdditionalField(additionalFields, Property.RATING, meta.get(TikaCoreProperties.RATING.getName()));
        addAdditionalField(additionalFields, Property.COMMENTS, meta.get(TikaCoreProperties.COMMENTS.getName()));

        // Surface partial-parse exceptions unconditionally: if Tika extracted some content but
        // also threw an exception (e.g. a truncated or malformed document), the caller needs to
        // know the result is incomplete. Not gated behind `properties` because silent data loss
        // is worse than an unexpected field.
        String containerException = meta.get(TikaCoreProperties.CONTAINER_EXCEPTION.getName());
        if (Strings.hasLength(containerException)) {
            additionalFields.put("exception", containerException);
        }

        ingestDocument.setFieldValue(targetField, additionalFields);
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

    public static final class Factory implements Processor.Factory {

        static final Set<Property> DEFAULT_PROPERTIES = EnumSet.allOf(Property.class);

        private final RelativeByteSizeValue maxFieldSizeFromNode;
        private final String maxFieldSizeExceededMessage;
        private final SetOnce<AttachmentIngestMetrics> attachmentMetrics;
        private final SetOnce<ExtractionBackend> backend;

        public Factory(Settings nodeSettings, SetOnce<AttachmentIngestMetrics> attachmentMetrics, SetOnce<ExtractionBackend> backend) {
            this.maxFieldSizeFromNode = MAX_FIELD_SIZE_SETTING.get(nodeSettings);
            this.maxFieldSizeExceededMessage = MAX_FIELD_SIZE_MESSAGE_SUFFIX_SETTING.get(nodeSettings);
            this.attachmentMetrics = attachmentMetrics;
            this.backend = backend;
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
                maxFieldSizeExceededMessage,
                attachmentMetrics,
                backend.get()
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
