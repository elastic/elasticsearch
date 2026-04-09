/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for verifying that fields are stored exactly where we expect them to. This helps identify when we're double storing.
 *
 * <p>A field's value can be stored in multiple ways:
 * <ul>
 *   <li><b>STORED_FIELD</b> - Lucene stored field</li>
 *   <li><b>DOC_VALUES</b> - Doc values</li>
 *   <li><b>IGNORED_SOURCE</b> - Stored in _ignored_source</li>
 * </ul>
 *
 * <p>Internally, the verifier checks both the primary field name and the fallback field (fieldName._original), and fails if the field is
 * stored at unexpected locations.
 */
public class FieldStorageVerifier {

    public enum StorageType {
        STORED_FIELD,
        DOC_VALUES,
        IGNORED_SOURCE
    }

    private final String fieldName;
    private final String fallbackFieldName;
    private final LuceneDocument document;

    private final EnumSet<StorageType> expectedStorageTypes = EnumSet.noneOf(StorageType.class);

    private FieldStorageVerifier(String fieldName, LuceneDocument document) {
        this.fieldName = fieldName;
        this.fallbackFieldName = fieldName + TextFamilyFieldType.FALLBACK_FIELD_NAME_SUFFIX;
        this.document = document;
    }

    public static FieldStorageVerifier forField(String fieldName, LuceneDocument document) {
        return new FieldStorageVerifier(fieldName, document);
    }

    public FieldStorageVerifier expectStoredField() {
        expectedStorageTypes.add(StorageType.STORED_FIELD);
        return this;
    }

    public FieldStorageVerifier expectDocValues() {
        expectedStorageTypes.add(StorageType.DOC_VALUES);
        return this;
    }

    public FieldStorageVerifier expectIgnoredSource() {
        expectedStorageTypes.add(StorageType.IGNORED_SOURCE);
        return this;
    }

    /**
     * Verifies that the field is stored as expected.
     */
    public void verify() {
        StorageLocations actualStorage = detectActualStorage();

        // first, check for double storage - each storage type should appear at most one location
        for (StorageType type : StorageType.values()) {
            Set<String> locations = actualStorage.fieldNamesFor(type);
            assertThat(
                Strings.format("Field '%s' has double storage for %s at locations: %s", fieldName, type, locations),
                locations.size(),
                lessThanOrEqualTo(1)
            );
        }

        // then verify that storage types match expectations
        assertThat(
            Strings.format("Field '%s' storage mismatch. Expected: %s, Actual: %s", fieldName, expectedStorageTypes, actualStorage),
            actualStorage.storageTypes(),
            equalTo(expectedStorageTypes)
        );
    }

    private StorageLocations detectActualStorage() {
        StorageLocations storage = new StorageLocations();

        // check primary field and fallback field
        checkFieldStorage(fieldName, storage);
        checkFieldStorage(fallbackFieldName, storage);

        // check ignored source
        checkIgnoredSource(fieldName, storage);
        checkIgnoredSource(fallbackFieldName, storage);

        return storage;
    }

    private void checkFieldStorage(String fieldName, StorageLocations storage) {
        for (IndexableField field : document.getFields(fieldName)) {
            if (field.fieldType().stored()) {
                storage.add(StorageType.STORED_FIELD, fieldName);
                assertThat(field, notNullValue());
            }
            if (field.fieldType().docValuesType() != DocValuesType.NONE) {
                storage.add(StorageType.DOC_VALUES, fieldName);
                assertThat(field, notNullValue());
            }
        }
    }

    private void checkIgnoredSource(String fieldName, StorageLocations storage) {
        List<IndexableField> ignoredSourceFields = document.getFields(IgnoredSourceFieldMapper.NAME);

        if (ignoredSourceFields.isEmpty()) {
            return;
        }

        for (IndexableField ignoredField : ignoredSourceFields) {
            // extract all entries from ignored source
            List<IgnoredSourceFieldMapper.NameValue> entries = IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.decode(
                ignoredField.binaryValue()
            );

            for (IgnoredSourceFieldMapper.NameValue entry : entries) {
                if (entry.name().equals(fieldName)) {
                    storage.add(StorageType.IGNORED_SOURCE, fieldName);
                    assertTrue(entry.hasValue());
                }
            }
        }
    }

    /**
     * An abstraction over a Map that is used to track each location the field is stored at.
     */
    private static class StorageLocations {
        private final EnumMap<StorageType, Set<String>> storageLocations = new EnumMap<>(StorageType.class);

        void add(StorageType type, String fieldName) {
            storageLocations.computeIfAbsent(type, k -> new HashSet<>()).add(fieldName);
        }

        EnumSet<StorageType> storageTypes() {
            if (storageLocations.isEmpty()) {
                return EnumSet.noneOf(StorageType.class);
            }
            return EnumSet.copyOf(storageLocations.keySet());
        }

        Set<String> fieldNamesFor(StorageType type) {
            return storageLocations.getOrDefault(type, Set.of());
        }

        @Override
        public String toString() {
            if (storageLocations.isEmpty()) {
                return "no storage";
            }
            StringBuilder sb = new StringBuilder();
            storageLocations.forEach((type, fields) -> {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(type).append(" in ").append(fields);
            });
            return sb.toString();
        }
    }

}
