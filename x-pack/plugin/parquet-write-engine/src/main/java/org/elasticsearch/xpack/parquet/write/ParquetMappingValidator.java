/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.parquet.write.file.BatchParquetWriter;
import org.elasticsearch.xpack.parquet.write.file.MultiValuedFields;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Checks the mapping of an index of the Parquet format when the index is created, so that the user sees a mapping that the engine cannot
 * store at once, and not at the first bulk or at the first flush. The mappings are the ones that the request and the matching templates
 * give; a field that a mapping update adds later is not seen here, and fails the bulk that writes it.
 *
 * <p>A field is refused when
 * <ul>
 *   <li>its type is not one that the engine writes, see {@link BatchParquetWriter#SUPPORTED_FIELD_TYPES}. Notably {@code date_nanos}:
 *       Iceberg tables of the format version 2 have no timestamp in nanoseconds, so the column could be written to a local file and
 *       never uploaded;</li>
 *   <li>it is indexed or has no doc values, because the documents live only in the Parquet files;</li>
 *   <li>it has multi-fields, which are other types of the same value (mostly {@code text}), which are indexed;</li>
 *   <li>it is a {@code boolean} or a {@code date} with several values: an array of them is not on the batch path.</li>
 * </ul>
 */
final class ParquetMappingValidator {

    private static final String OBJECT = "object";

    /** The supported types that cannot be lists: the batch path does not take an array of them (found by probing it). */
    private static final Set<String> NO_LISTS = Set.of("boolean", "date");

    private ParquetMappingValidator() {}

    static void validate(String formatName, List<CompressedXContent> mappings, boolean multiValueDefault) {
        for (CompressedXContent mapping : mappings) {
            final Map<String, Object> root = rootOf(parse(mapping));
            if (root.get("properties") instanceof Map<?, ?> properties) {
                validateProperties(formatName, "", properties, multiValueDefault);
            }
        }
    }

    private static Map<String, Object> parse(CompressedXContent mapping) {
        return XContentHelper.convertToMap(mapping.uncompressed(), true, XContentType.JSON).v2();
    }

    /** A mapping is the body of the type, or the body under the name of the type ({@code _doc}). */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> rootOf(Map<String, Object> mapping) {
        if (mapping.containsKey("properties") == false && mapping.size() == 1) {
            final Object only = mapping.values().iterator().next();
            if (only instanceof Map<?, ?> body) {
                return (Map<String, Object>) body;
            }
        }
        return mapping;
    }

    private static void validateProperties(String formatName, String prefix, Map<?, ?> properties, boolean multiValueDefault) {
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?> definition) {
                validateField(formatName, prefix + entry.getKey(), definition, multiValueDefault);
            }
        }
    }

    private static void validateField(String formatName, String path, Map<?, ?> definition, boolean multiValueDefault) {
        final Object type = definition.get("type");
        if (type == null || OBJECT.equals(type)) {
            if (definition.get("properties") instanceof Map<?, ?> children) {
                validateProperties(formatName, path + ".", children, multiValueDefault);
            }
            return;
        }
        if (BatchParquetWriter.SUPPORTED_FIELD_TYPES.contains(type.toString()) == false) {
            throw new IllegalArgumentException(
                formatName
                    + " does not support the type ["
                    + type
                    + "] of the field ["
                    + path
                    + "]; the supported types are "
                    + new TreeSet<>(BatchParquetWriter.SUPPORTED_FIELD_TYPES)
            );
        }
        if (NO_LISTS.contains(type.toString()) && MultiValuedFields.isMultiValued(definition, multiValueDefault)) {
            // an array of these is not on the batch path, so a document with one would fail its whole shard bulk
            throw new IllegalArgumentException(
                formatName
                    + " does not support a "
                    + type
                    + " field with several values, but the field ["
                    + path
                    + "] is multi-valued; set [multi_value] to false for it"
            );
        }
        if ("true".equals(String.valueOf(definition.get("index")))) {
            throw new IllegalArgumentException(
                formatName + " does not support an indexed field, but the field [" + path + "] sets [index] to true"
            );
        }
        if ("false".equals(String.valueOf(definition.get("doc_values")))) {
            throw new IllegalArgumentException(formatName + " needs doc values, but the field [" + path + "] sets [doc_values] to false");
        }
        if (definition.get("fields") instanceof Map<?, ?> fields && fields.isEmpty() == false) {
            throw new IllegalArgumentException(formatName + " does not support multi-fields, but the field [" + path + "] has some");
        }
    }
}
