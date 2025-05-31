/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public abstract class Mapper implements ToXContentFragment, Iterable<Mapper> {

    public static final String SYNTHETIC_SOURCE_KEEP_PARAM = "synthetic_source_keep";

    // Only relevant for synthetic source mode.
    public enum SourceKeepMode {
        NONE("none"),      // No source recording
        ARRAYS("arrays"),  // Store source for arrays of mapped fields
        ALL("all");        // Store source for both singletons and arrays of mapped fields

        SourceKeepMode(String name) {
            this.name = name;
        }

        static SourceKeepMode from(String input) {
            if (input == null) {
                input = "null";
            }
            if (input.equals(NONE.name)) {
                return NONE;
            }
            if (input.equals(ALL.name)) {
                return ALL;
            }
            if (input.equals(ARRAYS.name)) {
                return ARRAYS;
            }
            throw new IllegalArgumentException(
                "Unknown "
                    + SYNTHETIC_SOURCE_KEEP_PARAM
                    + " value ["
                    + input
                    + "], accepted values are ["
                    + String.join(",", Arrays.stream(SourceKeepMode.values()).map(SourceKeepMode::toString).toList())
                    + "]"
            );
        }

        @Override
        public String toString() {
            return name;
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field(SYNTHETIC_SOURCE_KEEP_PARAM, name);
        }

        private final String name;
    }

    // Only relevant for indexes configured with synthetic source mode. Otherwise, it has no effect.
    // Controls the default behavior for storing the source of leaf fields and objects, in singleton or array form.
    // Setting to SourceKeepMode.ALL is equivalent to disabling synthetic source, so this is not allowed.
    public static final Setting<SourceKeepMode> SYNTHETIC_SOURCE_KEEP_INDEX_SETTING = Setting.enumSetting(
        SourceKeepMode.class,
        settings -> {
            var indexMode = IndexSettings.MODE.get(settings);
            if (indexMode == IndexMode.LOGSDB) {
                return SourceKeepMode.ARRAYS.toString();
            } else {
                return SourceKeepMode.NONE.toString();
            }
        },
        "index.mapping.synthetic_source_keep",
        value -> {
            if (value == SourceKeepMode.ALL) {
                throw new IllegalArgumentException("index.mapping.synthetic_source_keep can't be set to [" + value + "]");
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.ServerlessPublic
    );

    public abstract static class Builder {

        private String leafName;

        protected Builder(String leafName) {
            this.leafName = leafName;
        }

        public final String leafName() {
            return this.leafName;
        }

        /** Returns a newly built mapper. */
        public abstract Mapper build(MapperBuilderContext context);

        void setLeafName(String leafName) {
            this.leafName = leafName;
        }
    }

    public interface TypeParser {
        Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext) throws MapperParsingException;

        /**
         * Whether we can parse this type on indices with the given index created version.
         */
        default boolean supportsVersion(IndexVersion indexCreatedVersion) {
            return indexCreatedVersion.onOrAfter(IndexVersions.MINIMUM_READONLY_COMPATIBLE);
        }
    }

    private final String leafName;

    @SuppressWarnings("this-escape")
    public Mapper(String leafName) {
        Objects.requireNonNull(leafName);
        this.leafName = internFieldName(leafName);
    }

    /**
     * Returns the name of the field.
     * When the field has a parent object, its leaf name won't include the entire path.
     * When subobjects are disabled, its leaf name will be the same as {@link #fullPath()} in practice, because its parent is the root.
     */
    public final String leafName() {
        return leafName;
    }

    /** Returns the canonical name which uniquely identifies the mapper against other mappers in a type. */
    public abstract String fullPath();

    /**
     * Returns a name representing the type of this mapper.
     */
    public abstract String typeName();

    /**
     * Return the merge of {@code mergeWith} into this.
     * Both {@code this} and {@code mergeWith} will be left unmodified.
     */
    public abstract Mapper merge(Mapper mergeWith, MapperMergeContext mapperMergeContext);

    /**
     * Validate any cross-field references made by this mapper
     * @param mappers a {@link MappingLookup} that can produce references to other mappers
     */
    public abstract void validate(MappingLookup mappers);

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static final StringLiteralDeduplicator fieldNameStringDeduplicator = new StringLiteralDeduplicator();

    /**
     * Interns the given field name string through a {@link StringLiteralDeduplicator}.
     * @param fieldName field name to intern
     * @return interned field name string
     */
    public static String internFieldName(String fieldName) {
        return fieldNameStringDeduplicator.deduplicate(fieldName);
    }

    private static final Map<FieldType, FieldType> fieldTypeDeduplicator = new ConcurrentHashMap<>();

    /**
     * Freezes the given {@link FieldType} instances and tries to deduplicate it as long as the field does not return a non-empty value for
     * {@link FieldType#getAttributes()}.
     *
     * @param fieldType field type to deduplicate
     * @return deduplicated field type
     */
    public static FieldType freezeAndDeduplicateFieldType(FieldType fieldType) {
        fieldType.freeze();
        var attributes = fieldType.getAttributes();
        if ((attributes != null && attributes.isEmpty() == false) || fieldType.getClass() != FieldType.class) {
            // don't deduplicate subclasses or types with non-empty attribute maps to avoid memory leaks
            return fieldType;
        }
        if (fieldTypeDeduplicator.size() > 1000) {
            // guard against the case where we run up too many combinations via (vector-)dimensions combinations
            fieldTypeDeduplicator.clear();
        }
        return fieldTypeDeduplicator.computeIfAbsent(fieldType, Function.identity());
    }

    /**
     * The total number of fields as defined in the mapping.
     * Defines how this mapper counts towards {@link MapperService#INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING}.
     */
    public abstract int getTotalFieldsCount();

    /**
     * @return whether this mapper supports storing leaf array elements natively when synthetic source is enabled.
     */
    public final boolean supportStoringArrayOffsets() {
        return getOffsetFieldName() != null;
    }

    /**
     * @return the offset field name used to store offsets iff {@link #supportStoringArrayOffsets()} returns
     * <code>true</code>.
     */
    public String getOffsetFieldName() {
        return null;
    }

    public abstract List<Mapper> getSourceMappers();
}
