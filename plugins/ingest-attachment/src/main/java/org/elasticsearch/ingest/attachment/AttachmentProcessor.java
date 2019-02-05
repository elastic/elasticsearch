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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.regex.Regex.simpleMatchToAutomaton;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class AttachmentProcessor extends AbstractProcessor {

    private static final Logger logger = LogManager.getLogger(AttachmentProcessor.class);

    static final String TYPE = "attachment";

    private static final int NUMBER_OF_CHARS_INDEXED = 100000;

    /**
     * Reserved keys
     */
    static final Set<String> RESERVED_PROPERTIES_KEYS =
        EnumSet.allOf(Property.class).stream().map(Property::toLowerCase).collect(Collectors.toSet());

    private final String field;
    private final String targetField;
    private final Set<String> properties;
    private final int indexedChars;
    private final boolean ignoreMissing;
    private final String indexedCharsField;
    private final CharacterRunAutomaton runAutomaton;

    AttachmentProcessor(String tag, String field, String targetField, Set<String> properties,
                        int indexedChars, boolean ignoreMissing, String indexedCharsField,
                        CharacterRunAutomaton automaton) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.properties = properties;
        this.indexedChars = indexedChars;
        this.ignoreMissing = ignoreMissing;
        this.indexedCharsField = indexedCharsField;
        this.runAutomaton = automaton;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        Map<String, Object> additionalFields = new HashMap<>();

        byte[] input = ingestDocument.getFieldValueAsBytes(field, ignoreMissing);

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
        String parsedContent = "";
        try {
            parsedContent = TikaImpl.parse(input, metadata, indexedChars);
        } catch (ZeroByteFileException e) {
            // tika 1.17 throws an exception when the InputStream has 0 bytes.
            // previously, it did not mind. This is here to preserve that behavior.
        } catch (Exception e) {
            throw new ElasticsearchParseException("Error parsing document in field [{}]", e, field);
        }

        if (properties.contains(Property.CONTENT.toLowerCase()) && Strings.hasLength(parsedContent)) {
            // somehow tika seems to append a newline at the end automatically, lets remove that again
            additionalFields.put(Property.CONTENT.toLowerCase(), parsedContent.trim());
        }

        if (properties.contains(Property.LANGUAGE.toLowerCase()) && Strings.hasLength(parsedContent)) {
            LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
            String language = identifier.getLanguage();
            additionalFields.put(Property.LANGUAGE.toLowerCase(), language);
        }

        if (properties.contains(Property.DATE.toLowerCase())) {
            String createdDate = metadata.get(TikaCoreProperties.CREATED);
            if (createdDate != null) {
                additionalFields.put(Property.DATE.toLowerCase(), createdDate);
            }
        }

        if (properties.contains(Property.TITLE.toLowerCase())) {
            String title = metadata.get(TikaCoreProperties.TITLE);
            if (Strings.hasLength(title)) {
                additionalFields.put(Property.TITLE.toLowerCase(), title);
            }
        }

        if (properties.contains(Property.AUTHOR.toLowerCase())) {
            String author = metadata.get("Author");
            if (Strings.hasLength(author)) {
                additionalFields.put(Property.AUTHOR.toLowerCase(), author);
            }
        }

        if (properties.contains(Property.KEYWORDS.toLowerCase())) {
            String keywords = metadata.get("Keywords");
            if (Strings.hasLength(keywords)) {
                additionalFields.put(Property.KEYWORDS.toLowerCase(), keywords);
            }
        }

        if (properties.contains(Property.CONTENT_TYPE.toLowerCase())) {
            String contentType = metadata.get(Metadata.CONTENT_TYPE);
            if (Strings.hasLength(contentType)) {
                additionalFields.put(Property.CONTENT_TYPE.toLowerCase(), contentType);
            }
        }

        if (properties.contains(Property.CONTENT_LENGTH.toLowerCase())) {
            String contentLength = metadata.get(Metadata.CONTENT_LENGTH);
            long length;
            if (Strings.hasLength(contentLength)) {
                length = Long.parseLong(contentLength);
            } else {
                length = parsedContent.length();
            }
            additionalFields.put(Property.CONTENT_LENGTH.toLowerCase(), length);
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

    Set<String> getProperties() {
        return properties;
    }

    CharacterRunAutomaton getRunAutomaton() {
        return runAutomaton;
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
            String indexedCharsField = readOptionalStringProperty(TYPE, processorTag, config, "indexed_chars_field");

            final Set<String> properties;

            CharacterRunAutomaton automaton = null;
            if (propertyNames != null) {
                properties = Sets.newHashSet(propertyNames);
                automaton = buildCharacterRunAutomaton(properties);
            } else {
                properties = RESERVED_PROPERTIES_KEYS;
            }

            return new AttachmentProcessor(processorTag, field, targetField, properties, indexedChars, ignoreMissing, indexedCharsField,
                automaton);
        }
    }

    static CharacterRunAutomaton buildCharacterRunAutomaton(Set<String> regex) {
        // We build the regex automaton we will use to extract raw metadata
        if (regex.isEmpty()) {
            return null;
        }
        return new CharacterRunAutomaton(
            MinimizationOperations.minimize(
                simpleMatchToAutomaton(regex.toArray(new String[]{})), Operations.DEFAULT_MAX_DETERMINIZED_STATES));

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

        public String toLowerCase() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
