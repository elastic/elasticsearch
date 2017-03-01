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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.regex.Regex.simpleMatchToAutomaton;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class AttachmentProcessor extends AbstractProcessor {

    private static final Logger logger = ESLoggerFactory.getLogger(AttachmentProcessor.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    static final String TYPE = "attachment";

    private static final int NUMBER_OF_CHARS_INDEXED = 100000;

    // Generated static fields. Will be used as reserved key names with _ prefix and suffix like _content_
    static final String CONTENT = "content";
    static final String TITLE = "title";
    static final String AUTHOR = "author";
    static final String KEYWORDS = "keywords";
    static final String DATE = "date";
    static final String CONTENT_TYPE = "content_type";
    static final String CONTENT_LENGTH = "content_length";
    static final String LANGUAGE = "language";

    /**
     * Static generated field names
     */
    static final Set<String> RESERVED_PROPERTIES = Sets.newHashSet(
        CONTENT,
        TITLE,
        AUTHOR,
        KEYWORDS,
        DATE,
        CONTENT_TYPE,
        CONTENT_LENGTH,
        LANGUAGE
    );

    /**
     * Reserved keys
     */
    static final Set<String> RESERVED_PROPERTIES_KEYS =
        RESERVED_PROPERTIES.stream().map(AttachmentProcessor::asReservedProperty).collect(Collectors.toSet());

    /**
     * Transform a generated field name to a reserved key with _ prefix and suffix
     * @param property the generated static field name (content for example)
     * @return the reserved key name corresponding to the field name (_content_ for example)
     */
    static String asReservedProperty(String property) {
        return "_" + property + "_";
    }

    private final String field;
    private final String targetField;
    private final Set<String> reservedProperties;
    private final Set<String> properties;
    private final int indexedChars;
    private final boolean ignoreMissing;
    private final CharacterRunAutomaton runAutomaton;

    AttachmentProcessor(String tag, String field, String targetField, Set<String> reservedProperties, Set<String> properties,
                        int indexedChars, boolean ignoreMissing, CharacterRunAutomaton automaton) throws IOException {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.properties = properties;
        this.reservedProperties = reservedProperties;
        this.indexedChars = indexedChars;
        this.ignoreMissing = ignoreMissing;
        this.runAutomaton = automaton;
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

            if (reservedProperties.contains(asReservedProperty(CONTENT)) && Strings.hasLength(parsedContent)) {
                // somehow tika seems to append a newline at the end automatically, lets remove that again
                additionalFields.put(CONTENT, parsedContent.trim());
            }

            if (reservedProperties.contains(asReservedProperty(LANGUAGE)) && Strings.hasLength(parsedContent)) {
                LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
                String language = identifier.getLanguage();
                additionalFields.put(LANGUAGE, language);
            }

            if (reservedProperties.contains(asReservedProperty(DATE))) {
                String createdDate = metadata.get(TikaCoreProperties.CREATED);
                if (createdDate != null) {
                    additionalFields.put(DATE, createdDate);
                }
            }

            if (reservedProperties.contains(asReservedProperty(TITLE))) {
                String title = metadata.get(TikaCoreProperties.TITLE);
                if (Strings.hasLength(title)) {
                    additionalFields.put(TITLE, title);
                }
            }

            if (reservedProperties.contains(asReservedProperty(AUTHOR))) {
                String author = metadata.get("Author");
                if (Strings.hasLength(author)) {
                    additionalFields.put(AUTHOR, author);
                }
            }

            if (reservedProperties.contains(asReservedProperty(KEYWORDS))) {
                String keywords = metadata.get("Keywords");
                if (Strings.hasLength(keywords)) {
                    additionalFields.put(KEYWORDS, keywords);
                }
            }

            if (reservedProperties.contains(asReservedProperty(CONTENT_TYPE))) {
                String contentType = metadata.get(Metadata.CONTENT_TYPE);
                if (Strings.hasLength(contentType)) {
                    additionalFields.put(CONTENT_TYPE, contentType);
                }
            }

            if (reservedProperties.contains(asReservedProperty(CONTENT_LENGTH))) {
                String contentLength = metadata.get(Metadata.CONTENT_LENGTH);
                long length;
                if (Strings.hasLength(contentLength)) {
                    length = Long.parseLong(contentLength);
                } else {
                    length = parsedContent.length();
                }
                additionalFields.put(CONTENT_LENGTH, length);
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

    Set<String> getReservedProperties() {
        return reservedProperties;
    }

    int getIndexedChars() {
        return indexedChars;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public AttachmentProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                          Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "attachment");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            int indexedChars = readIntProperty(TYPE, processorTag, config, "indexed_chars", NUMBER_OF_CHARS_INDEXED);
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            final Set<String> reservedProperties;
            final Set<String> properties = new HashSet<>();
            if (propertyNames != null) {
                reservedProperties = Sets.newHashSet();
                for (String fieldName : propertyNames) {
                    // We build the regex automaton we will use to extract raw metadata
                    CharacterRunAutomaton automaton = new CharacterRunAutomaton(simpleMatchToAutomaton(fieldName));

                    // Let's see if available reserved properties match what the user asked for
                    for (String reservedProperty : RESERVED_PROPERTIES) {
                        String reservedPropertyKey = asReservedProperty(reservedProperty);
                        if (automaton.run(reservedPropertyKey)) {
                            logger.trace("found a reserved property: [{}]", reservedPropertyKey);
                            reservedProperties.add(reservedPropertyKey);
                        } else if (fieldName.equals(reservedProperty)) {
                            deprecationLogger.deprecated("[{}] should be replaced with [{}]", reservedProperty, reservedPropertyKey);
                            reservedProperties.add(reservedPropertyKey);
                        }
                    }

                    // And add the property
                    properties.add(fieldName);
                }
            } else {
                reservedProperties = RESERVED_PROPERTIES_KEYS;
                logger.trace("no properties provided, falling back to default reserved properties: [{}]", reservedProperties);
            }

            // We build the regex automaton we will use to extract raw metadata
            CharacterRunAutomaton automaton;
            if (properties.isEmpty()) {
                automaton = null;
            } else {
                automaton = new CharacterRunAutomaton(simpleMatchToAutomaton(properties.toArray(new String[]{})));
            }

            return new AttachmentProcessor(processorTag, field, targetField, reservedProperties, properties, indexedChars, ignoreMissing,
                automaton);
        }
    }
}
