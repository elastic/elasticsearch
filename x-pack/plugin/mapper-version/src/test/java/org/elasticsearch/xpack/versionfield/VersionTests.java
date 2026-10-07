/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class VersionTests extends ESTestCase {

    public void testStringCtorOrderingSemver() {
        assertTrue(new Version("1").compareTo(new Version("1.0")) < 0);
        assertTrue(new Version("1.0").compareTo(new Version("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("2.0.0")) < 0);
        assertTrue(new Version("2.0.0").compareTo(new Version("11.0.0")) < 0);
        assertTrue(new Version("2.0.0").compareTo(new Version("2.1.0")) < 0);
        assertTrue(new Version("2.1.0").compareTo(new Version("2.1.1")) < 0);
        assertTrue(new Version("2.1.1").compareTo(new Version("2.1.1.0")) < 0);
        assertTrue(new Version("2.0.0").compareTo(new Version("11.0.0")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("2.0")) < 0);
        assertTrue(new Version("1.0.0-a").compareTo(new Version("1.0.0-b")) < 0);
        assertTrue(new Version("1.0.0-1.0.0").compareTo(new Version("1.0.0-2.0")) < 0);
        assertTrue(new Version("1.0.0-alpha").compareTo(new Version("1.0.0-alpha.1")) < 0);
        assertTrue(new Version("1.0.0-alpha.1").compareTo(new Version("1.0.0-alpha.beta")) < 0);
        assertTrue(new Version("1.0.0-alpha.beta").compareTo(new Version("1.0.0-beta")) < 0);
        assertTrue(new Version("1.0.0-beta").compareTo(new Version("1.0.0-beta.2")) < 0);
        assertTrue(new Version("1.0.0-beta.2").compareTo(new Version("1.0.0-beta.11")) < 0);
        assertTrue(new Version("1.0.0-beta11").compareTo(new Version("1.0.0-beta2")) < 0); // correct according to Semver specs
        assertTrue(new Version("1.0.0-beta.11").compareTo(new Version("1.0.0-rc.1")) < 0);
        assertTrue(new Version("1.0.0-rc.1").compareTo(new Version("1.0.0")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("2.0.0-pre127")) < 0);
        assertTrue(new Version("2.0.0-pre127").compareTo(new Version("2.0.0-pre128")) < 0);
        assertTrue(new Version("2.0.0-pre128").compareTo(new Version("2.0.0-pre128-somethingelse")) < 0);
        assertTrue(new Version("2.0.0-pre20201231z110026").compareTo(new Version("2.0.0-pre227")) < 0);
        // invalid versions sort after valid ones
        assertTrue(new Version("99999.99999.99999").compareTo(new Version("1.invalid")) < 0);
        assertTrue(new Version("").compareTo(new Version("a")) < 0);
    }

    public void testBytesRefCtorOrderingSemver() {
        assertTrue(new Version(encodeVersion("1")).compareTo(new Version(encodeVersion("1.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0")).compareTo(new Version(encodeVersion("1.0.0.0.0.0.0.0.0.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("1.0.0.0.0.0.0.0.0.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("2.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0")).compareTo(new Version(encodeVersion("11.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0")).compareTo(new Version(encodeVersion("2.1.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.1.0")).compareTo(new Version(encodeVersion("2.1.1"))) < 0);
        assertTrue(new Version(encodeVersion("2.1.1")).compareTo(new Version(encodeVersion("2.1.1.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0")).compareTo(new Version(encodeVersion("11.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("2.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-a")).compareTo(new Version(encodeVersion("1.0.0-b"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-1.0.0")).compareTo(new Version(encodeVersion("1.0.0-2.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-alpha")).compareTo(new Version(encodeVersion("1.0.0-alpha.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-alpha.1")).compareTo(new Version(encodeVersion("1.0.0-alpha.beta"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-alpha.beta")).compareTo(new Version(encodeVersion("1.0.0-beta"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-beta")).compareTo(new Version(encodeVersion("1.0.0-beta.2"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-beta.2")).compareTo(new Version(encodeVersion("1.0.0-beta.11"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-beta11")).compareTo(new Version(encodeVersion("1.0.0-beta2"))) < 0); // correct
                                                                                                                         // according to
                                                                                                                         // Semver specs
        assertTrue(new Version(encodeVersion("1.0.0-beta.11")).compareTo(new Version(encodeVersion("1.0.0-rc.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-rc.1")).compareTo(new Version(encodeVersion("1.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("2.0.0-pre127"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0-pre127")).compareTo(new Version(encodeVersion("2.0.0-pre128"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0-pre128")).compareTo(new Version(encodeVersion("2.0.0-pre128-somethingelse"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0-pre20201231z110026")).compareTo(new Version(encodeVersion("2.0.0-pre227"))) < 0);
        // invalid versions sort after valid ones
        assertTrue(new Version(encodeVersion("99999.99999.99999")).compareTo(new Version(encodeVersion("1.invalid"))) < 0);
        assertTrue(new Version(encodeVersion("")).compareTo(new Version(encodeVersion("a"))) < 0);
    }

    public void testConstructorsComparison() {
        assertTrue(new Version(encodeVersion("1")).compareTo(new Version("1")) == 0);
        assertTrue(new Version(encodeVersion("1.2.3")).compareTo(new Version("1.2.3")) == 0);
        assertTrue(new Version(encodeVersion("1.2.3-rc1")).compareTo(new Version("1.2.3-rc1")) == 0);
        assertTrue(new Version(encodeVersion("lkjlaskdjf")).compareTo(new Version("lkjlaskdjf")) == 0);
        assertTrue(new Version(encodeVersion("99999.99999.99999")).compareTo(new Version("99999.99999.99999")) == 0);
    }

    private static BytesRef encodeVersion(String version) {
        return VersionEncoder.encodeVersion(version).bytesRef;
    }

    public void testDocValueReadEstimators() throws IOException {
        VersionStringDocValuesField field = versionField("1.2.3", "2.0.0-alpha.1");
        VersionScriptDocValues values = (VersionScriptDocValues) field.toScriptDocValues();

        // The encoding is at least as long as the decoded text, so the estimate covers the String the read builds.
        for (int i = 0; i < 2; i++) {
            long encoded = field.encodedLength(i);
            assertTrue(encoded >= field.getInternal(i).length());
            long expected = ((16 + encoded + 7) & ~7L) + 32 + 32 + 2 * encoded;
            assertEquals(expected, VersionAllocationEstimators.versionReadBytes(values, i));
            assertEquals(expected, VersionAllocationEstimators.versionStringBytes(field, i, null));
            assertEquals(expected + 32, VersionAllocationEstimators.versionObjectBytes(field, i, null));
        }
        assertEquals(VersionAllocationEstimators.versionReadBytes(values, 0), VersionAllocationEstimators.versionReadBytes(values));
        assertEquals(
            32 + ((16 + 8 * 2 + 7) & ~7L) + VersionAllocationEstimators.versionReadBytes(values, 0) + VersionAllocationEstimators
                .versionReadBytes(values, 1),
            VersionAllocationEstimators.versionStringsBytes(field)
        );

        // Out of range reads throw or return the default, so they cost nothing; neither does an empty field.
        assertEquals(0, VersionAllocationEstimators.versionReadBytes(values, 2));
        assertEquals(0, VersionAllocationEstimators.versionObjectBytes(field, -1, null));
        VersionStringDocValuesField empty = versionField();
        assertEquals(0, VersionAllocationEstimators.versionStringsBytes(empty));
        assertEquals(0, VersionAllocationEstimators.versionStringBytes(empty, null));
    }

    /** A version field over one document holding {@code versions}, already on that document. */
    private static VersionStringDocValuesField versionField(String... versions) throws IOException {
        BytesRef[] encoded = new BytesRef[versions.length];
        for (int i = 0; i < versions.length; i++) {
            encoded[i] = VersionEncoder.encodeVersion(versions[i]).bytesRef;
        }
        SortedSetDocValues docValues = new SortedSetDocValues() {
            private int next;

            @Override
            public boolean advanceExact(int target) {
                next = 0;
                return target == 0 && encoded.length > 0;
            }

            @Override
            public long nextOrd() {
                return next++;
            }

            @Override
            public int docValueCount() {
                return encoded.length;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return encoded[(int) ord];
            }

            @Override
            public long getValueCount() {
                return encoded.length;
            }

            @Override
            public int docID() {
                return 0;
            }

            @Override
            public int nextDoc() {
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 1;
            }
        };
        VersionStringDocValuesField field = new VersionStringDocValuesField(docValues, "test");
        field.setNextDocId(0);
        return field;
    }
}
