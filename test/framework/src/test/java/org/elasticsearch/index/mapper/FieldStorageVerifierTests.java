/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class FieldStorageVerifierTests extends ESTestCase {

    public void testVerifyPassesWhenNoStorageExpectedAndNothingStored() {
        LuceneDocument doc = new LuceneDocument();
        FieldStorageVerifier.forField("myfield", doc).verify();
    }

    public void testVerifyPassesWhenStoredFieldExpectedAndFieldIsStored() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "test value"));

        FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify();
    }

    public void testVerifyPassesWhenDocValuesExpectedAndDocValuesExist() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("test value")));

        FieldStorageVerifier.forField("myfield", doc).expectDocValues().verify();
    }

    public void testVerifyPassesWhenStoredFieldExpectedInFallbackField() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield._original", "test value"));

        FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify();
    }

    public void testVerifyPassesWhenDocValuesExpectedInFallbackField() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new BinaryDocValuesField("myfield._original", new BytesRef("test value")));

        FieldStorageVerifier.forField("myfield", doc).expectDocValues().verify();
    }

    public void testVerifyFailsWhenStoredFieldExpectedButNotPresent() {
        LuceneDocument doc = new LuceneDocument();

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenDocValuesExpectedButNotPresent() {
        LuceneDocument doc = new LuceneDocument();

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectDocValues().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenNoStorageExpectedButStoredFieldPresent() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "test value"));

        AssertionError error = expectThrows(AssertionError.class, () -> FieldStorageVerifier.forField("myfield", doc).verify());

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenNoStorageExpectedButDocValuesPresent() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("test value")));

        AssertionError error = expectThrows(AssertionError.class, () -> FieldStorageVerifier.forField("myfield", doc).verify());

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenStoredFieldExpectedButDocValuesPresent() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("test value")));

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenDocValuesExpectedButStoredFieldPresent() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "test value"));

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectDocValues().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsOnDoubleStorageSameType() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "value1"));
        doc.add(new StoredField("myfield._original", "value2"));

        // should fail - field is stored twice
        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("double storage"));
    }

    public void testVerifyFailsOnDoubleStorageDocValues() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("value1")));
        doc.add(new BinaryDocValuesField("myfield._original", new BytesRef("value2")));

        // should fail - field is stored twice
        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectDocValues().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("double storage"));
    }

    public void testVerifyFailsWhenStoredFieldExpectedButDoubledStoredInDocValues() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "potato"));
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("potato")));

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenDocValuesExpectedButDoubledStoredInStoredField() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "potato"));
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("potato")));

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectDocValues().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyPassesWithMultipleExpectedStorageTypes() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "potato"));
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("potato")));

        // should pass - expecting both stored field and doc values
        FieldStorageVerifier.forField("myfield", doc).expectStoredField().expectDocValues().verify();
    }

    public void testVerifyPassesWithMultiValuedStoredField() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "potato1"));
        doc.add(new StoredField("myfield", "potato2"));
        doc.add(new StoredField("myfield", "potato3"));

        FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify();
    }

    public void testVerifyFailsWhenOnlyOneOfMultipleExpectedTypesPresent() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "test value"));

        // should fail - expecting both stored field and doc values, but only stored field present
        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectStoredField().expectDocValues().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyPassesWhenIgnoredSourceExpectedAndFieldIsInIgnoredSource() {
        LuceneDocument doc = new LuceneDocument();
        addToIgnoredSource(doc, "myfield", "test value");

        FieldStorageVerifier.forField("myfield", doc).expectIgnoredSource().verify();
    }

    public void testVerifyFailsWhenIgnoredSourceExpectedButNotPresent() {
        LuceneDocument doc = new LuceneDocument();

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectIgnoredSource().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenNoStorageExpectedButIgnoredSourcePresent() {
        LuceneDocument doc = new LuceneDocument();
        addToIgnoredSource(doc, "myfield", "test value");

        // expecting no storage, but field is in ignored source
        AssertionError error = expectThrows(AssertionError.class, () -> FieldStorageVerifier.forField("myfield", doc).verify());

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenStoredFieldExpectedButDoubleStoredInIgnoredSource() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new StoredField("myfield", "potato1"));
        addToIgnoredSource(doc, "myfield", "potato2");

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectStoredField().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    public void testVerifyFailsWhenDocValuesExpectedButDoubleStoredInIgnoredSource() {
        LuceneDocument doc = new LuceneDocument();
        doc.add(new BinaryDocValuesField("myfield", new BytesRef("potato1")));
        addToIgnoredSource(doc, "myfield", "potato2");

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> FieldStorageVerifier.forField("myfield", doc).expectDocValues().verify()
        );

        assertThat(error.getMessage(), containsString("myfield"));
        assertThat(error.getMessage(), containsString("storage mismatch"));
    }

    private void addToIgnoredSource(LuceneDocument doc, String fieldName, String value) {
        var nameValue = new IgnoredSourceFieldMapper.NameValue(fieldName, 0, new BytesRef(value), doc);
        BytesRef encoded = IgnoredSourceFieldMapper.CoalescedIgnoredSourceEncoding.encode(List.of(nameValue));
        doc.add(new StoredField(IgnoredSourceFieldMapper.NAME, encoded));
    }
}
