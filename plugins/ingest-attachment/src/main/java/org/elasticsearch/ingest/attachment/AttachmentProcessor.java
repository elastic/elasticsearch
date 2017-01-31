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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.regex.Regex.simpleMatchToAutomaton;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.ingest.attachment.AttachmentProcessor.ReservedProperty.findDeprecatedProperty;

public final class AttachmentProcessor extends AbstractProcessor {

    private static final Logger logger = ESLoggerFactory.getLogger(AttachmentProcessor.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final String TYPE = "attachment";

    private static final int NUMBER_OF_CHARS_INDEXED = 100000;

    private final String field;
    private final String targetField;
    private final Set<ReservedProperty> reservedProperties;
    private final Set<String> properties;
    private final int indexedChars;
    private final boolean ignoreMissing;
    private final CharacterRunAutomaton runAutomaton;

    AttachmentProcessor(String tag, String field, String targetField, Set<ReservedProperty> reservedProperties,
                        Set<String> properties, int indexedChars, boolean ignoreMissing) throws IOException {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.properties = properties;
        this.reservedProperties = reservedProperties;
        this.indexedChars = indexedChars;
        this.ignoreMissing = ignoreMissing;

        // We build the regex automaton we will use to extract raw metadata
        if (properties.isEmpty()) {
            this.runAutomaton = null;
        } else {
            this.runAutomaton = new CharacterRunAutomaton(simpleMatchToAutomaton(properties.toArray(new String[]{})));
        }
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        Map<String, Object> additionalFields = new HashMap<>();

        byte[] input = ingestDocument.getFieldValueAsBytes(field, ignoreMissing);

        if (input == null && ignoreMissing) {
            return;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse.");
        }

        try {
            Metadata metadata = new Metadata();
            String parsedContent = TikaImpl.parse(input, metadata, indexedChars);

            if (reservedProperties.contains(ReservedProperty.CONTENT) && Strings.hasLength(parsedContent)) {
                // somehow tika seems to append a newline at the end automatically, lets remove that again
                additionalFields.put(ReservedProperty.CONTENT.toLowerCase(), parsedContent.trim());
            }

            if (reservedProperties.contains(ReservedProperty.LANGUAGE) && Strings.hasLength(parsedContent)) {
                LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
                String language = identifier.getLanguage();
                additionalFields.put(ReservedProperty.LANGUAGE.toLowerCase(), language);
            }

            if (reservedProperties.contains(ReservedProperty.DATE)) {
                String createdDate = metadata.get(TikaCoreProperties.CREATED);
                if (createdDate != null) {
                    additionalFields.put(ReservedProperty.DATE.toLowerCase(), createdDate);
                }
            }

            if (reservedProperties.contains(ReservedProperty.TITLE)) {
                String title = metadata.get(TikaCoreProperties.TITLE);
                if (Strings.hasLength(title)) {
                    additionalFields.put(ReservedProperty.TITLE.toLowerCase(), title);
                }
            }

            if (reservedProperties.contains(ReservedProperty.AUTHOR)) {
                String author = metadata.get("Author");
                if (Strings.hasLength(author)) {
                    additionalFields.put(ReservedProperty.AUTHOR.toLowerCase(), author);
                }
            }

            if (reservedProperties.contains(ReservedProperty.KEYWORDS)) {
                String keywords = metadata.get("Keywords");
                if (Strings.hasLength(keywords)) {
                    additionalFields.put(ReservedProperty.KEYWORDS.toLowerCase(), keywords);
                }
            }

            if (reservedProperties.contains(ReservedProperty.CONTENT_TYPE)) {
                String contentType = metadata.get(Metadata.CONTENT_TYPE);
                if (Strings.hasLength(contentType)) {
                    additionalFields.put(ReservedProperty.CONTENT_TYPE.toLowerCase(), contentType);
                }
            }

            if (reservedProperties.contains(ReservedProperty.CONTENT_LENGTH)) {
                String contentLength = metadata.get(Metadata.CONTENT_LENGTH);
                long length;
                if (Strings.hasLength(contentLength)) {
                    length = Long.parseLong(contentLength);
                } else {
                    length = parsedContent.length();
                }
                additionalFields.put(ReservedProperty.CONTENT_LENGTH.toLowerCase(), length);
            }

            // If we asked for other raw metadata
            if (runAutomaton != null) {
                for (String metadataName : metadata.names()) {
                    String value = metadata.get(metadataName);
                    logger.trace("found metadata [{}:{}]", metadataName, value);
                    if (runAutomaton.run(metadataName)) {
                        logger.trace("metadata [{}] matched one of the properties", metadataName);
                        additionalFields.put(metadataName, value);
                    }
                }
            }
        } catch (Exception e) {
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

    Set<String> getProperties() {
        return properties;
    }

    public Set<ReservedProperty> getReservedProperties() {
        return reservedProperties;
    }

    int getIndexedChars() {
        return indexedChars;
    }

    public static final class Factory implements Processor.Factory {

        static final Set<ReservedProperty> DEFAULT_PROPERTIES = EnumSet.allOf(ReservedProperty.class);

        @Override
        public AttachmentProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                          Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "attachment");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            int indexedChars = readIntProperty(TYPE, processorTag, config, "indexed_chars", NUMBER_OF_CHARS_INDEXED);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            final Set<ReservedProperty> reservedProperties;
            final Set<String> properties = new HashSet<>();
            if (propertyNames != null) {
                reservedProperties = EnumSet.noneOf(ReservedProperty.class);
                for (String fieldName : propertyNames) {
                    try {
                        reservedProperties.add(ReservedProperty.parse(fieldName));
                        logger.trace("found a reserved property: [{}]", fieldName);
                    } catch (Exception e) {
                        // This is expected when the user does not define a reserved property, so let's just ignore this
                        // Let's try to see if the user defined a deprecated property name (like xxx instead of _xxx_)
                        ReservedProperty reservedProperty = findDeprecatedProperty(fieldName);
                        if (reservedProperty != null) {
                            deprecationLogger.deprecated("[{}] should be replaced with [{}]", reservedProperty.toLowerCase(),
                                reservedProperty.key);
                            reservedProperties.add(reservedProperty);
                        } else {
                            // It's not a reserved property, so let's add it as a user provided property
                            logger.trace("found a user provided property: [{}]", fieldName);
                            properties.add(fieldName);
                        }
                    }
                }
            } else {
                reservedProperties = DEFAULT_PROPERTIES;
                logger.trace("no properties provided, falling back to default reserved properties: [{}]", reservedProperties);
            }

            return new AttachmentProcessor(processorTag, field, targetField, reservedProperties, properties, indexedChars, ignoreMissing);
        }
    }

    enum ReservedProperty {

        CONTENT("_content_"),
        TITLE("_title_"),
        AUTHOR("_author_"),
        KEYWORDS("_keywords_"),
        DATE("_date_"),
        CONTENT_TYPE("_content_type_"),
        CONTENT_LENGTH("_content_length_"),
        LANGUAGE("_language_");

        String key;

        ReservedProperty(String key) {
            this.key = key;
        }

        public static ReservedProperty parse(String value) {
            EnumSet<ReservedProperty> reservedProperties = EnumSet.allOf(ReservedProperty.class);
            for (ReservedProperty reservedProperty : reservedProperties) {
                if (reservedProperty.key.equals(value)) {
                    return reservedProperty;
                }
            }
            throw new IllegalArgumentException(value + " is not one of the known keys");
        }

        public static ReservedProperty findDeprecatedProperty(String value) {
            EnumSet<ReservedProperty> reservedProperties = EnumSet.allOf(ReservedProperty.class);
            for (ReservedProperty reservedProperty : reservedProperties) {
                if (reservedProperty.toLowerCase().equals(value)) {
                    return reservedProperty;
                }
            }
            return null;
        }

        public String toLowerCase() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
