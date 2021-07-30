/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.common.FingerprintProcessor.DELIMITER;
import static org.elasticsearch.ingest.common.FingerprintProcessor.toBytes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FingerprintProcessorTests extends ESTestCase {

    public void testBasic() throws Exception {
        var fields = new ArrayList<String>();
        fields.add("foo");
        fields.add("bar");

        var inputMap = new LinkedHashMap<String, Object>();
        inputMap.put("foo", "fooValue");
        inputMap.put("bar", "barValue");

        List<Object> expectedValues = List.of("barValue", "fooValue");

        doTestFingerprint(inputMap, fields, expectedValues, "IgxzmZVknx4+Og/eUpvIlqH9PdI=");
    }

    public void testFieldsAreConsistentlyOrdered() throws Exception {
        List<String> fieldList = randomList(1, 10, () -> randomAlphaOfLength(8));
        List<String> sortedFieldList = new ArrayList<>(fieldList);
        sortedFieldList.sort(Comparator.naturalOrder());

        var sortedInputMap = new LinkedHashMap<String, Object>();
        List<Object> expectedValues = new ArrayList<>();
        for (String s : sortedFieldList) {
            sortedInputMap.put(s, s);
            expectedValues.add(s);
        }
        String sortedFingerprint = doTestFingerprint(sortedInputMap, sortedFieldList, expectedValues, null);

        var shuffledInputMap = new LinkedHashMap<String, Object>();
        for (String s : fieldList) {
            shuffledInputMap.put(s, s);
        }
        String shuffledFingerprint = doTestFingerprint(shuffledInputMap, fieldList, expectedValues, null);

        assertThat(sortedFingerprint, equalTo(shuffledFingerprint));
    }

    public void testMapEntriesAreConsistentlyOrdered() throws Exception {
        List<String> keyList = randomList(1, 10, () -> randomAlphaOfLength(8));
        List<String> sortedKeyList = new ArrayList<>(keyList);
        sortedKeyList.sort(Comparator.naturalOrder());

        var sortedInputMap = new LinkedHashMap<String, Object>();
        List<Object> expectedValues = new ArrayList<>();
        for (String s : sortedKeyList) {
            sortedInputMap.put(s, s);
            expectedValues.add(s);
            expectedValues.add(s);
        }
        var docMap = new HashMap<String, Object>();
        docMap.put("map", sortedInputMap);
        String sortedFingerprint = doTestFingerprint(docMap, List.of("map"), expectedValues, null);

        var shuffledInputMap = new LinkedHashMap<String, Object>();
        for (String s : keyList) {
            shuffledInputMap.put(s, s);
        }
        docMap = new HashMap<String, Object>();
        docMap.put("map", shuffledInputMap);
        String shuffledFingerprint = doTestFingerprint(docMap, List.of("map"), expectedValues, null);

        assertThat(sortedFingerprint, equalTo(shuffledFingerprint));
    }

    public void testIgnoreMissing() throws Exception {
        // only one value contributes to fingerprint
        var docMap = new HashMap<String, Object>();
        docMap.put("foo", "foo");
        doTestFingerprint(docMap, List.of("foo", "bar", "baz"), List.of("foo"), "WoyqQDn9vALAGmScjA9Z2yg7sos=", true);

        // two values contribute to fingerprint
        docMap = new HashMap<>();
        docMap.put("foo", "foo");
        docMap.put("bar", "foo");
        doTestFingerprint(docMap, List.of("foo", "bar", "baz"), List.of("foo", "foo"), "vjq2RyU5UA8vzeM5gIbfrOGir7w=", true);

        // three values contribute to fingerprint
        docMap = new HashMap<>();
        docMap.put("foo", "foo");
        docMap.put("bar", "foo");
        docMap.put("baz", "foo");
        doTestFingerprint(docMap, List.of("foo", "bar", "baz"), List.of("foo", "foo", "foo"), "2Ozd89kaee2AnbrjU8zB6QGn9Wo=", true);

        // error when ignore_missing is false
        final var docMap2 = new HashMap<String, Object>();
        docMap2.put("foo", "foo");
        docMap2.put("bar", "foo");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> doTestFingerprint(docMap2, List.of("foo", "bar", "baz"), List.of("foo"), null, false, null)
        );
        assertThat(e.getMessage(), containsString("missing field [baz] when calculating fingerprint"));
    }

    public void testDataTypes() throws Exception {
        var typesMap = new HashMap<String, Object>();
        typesMap.put("0string", "foo");
        typesMap.put("1byte[]", new byte[] { 0, 1, 2 });
        typesMap.put("2integer", 42);
        typesMap.put("3long", 43L);
        typesMap.put("4float", 3.14F);
        typesMap.put("5double", 3.15D);
        typesMap.put("6boolean", true);
        typesMap.put("7ZonedDateTime", ZonedDateTime.now());
        typesMap.put("8date", Date.from(Instant.now()));
        typesMap.put("9null", null);

        List<Object> expectedValues = new ArrayList<>();
        expectedValues.add("0string");
        expectedValues.add("foo");
        expectedValues.add("1byte[]");
        expectedValues.add(new byte[] { 0, 1, 2 });
        expectedValues.add("2integer");
        expectedValues.add(42);
        expectedValues.add("3long");
        expectedValues.add(43L);
        expectedValues.add("4float");
        expectedValues.add(3.14F);
        expectedValues.add("5double");
        expectedValues.add(3.15D);
        expectedValues.add("6boolean");
        expectedValues.add(true);
        expectedValues.add("7ZonedDateTime");
        expectedValues.add(typesMap.get("7ZonedDateTime"));
        expectedValues.add("8date");
        expectedValues.add(typesMap.get("8date"));
        expectedValues.add("9null");
        expectedValues.add(null);

        var docMap = new HashMap<String, Object>();
        docMap.put("types", typesMap);
        doTestFingerprint(docMap, List.of("types"), expectedValues, null);
    }

    public void testSalt() throws Exception {
        var inputMap = new LinkedHashMap<String, Object>();
        inputMap.put("foo", "foo");
        doTestFingerprint(inputMap, List.of("foo"), List.of("foo"), "rWTTCYvRPQAzKXydmKwyC+//dmM=", "salt");
    }

    private String doTestFingerprint(
        Map<String, Object> inputMap,
        List<String> fields,
        List<Object> expectedValues,
        String expectedFingerprint,
        String salt
    ) throws Exception {
        return doTestFingerprint(inputMap, fields, expectedValues, expectedFingerprint, false, salt);
    }

    private String doTestFingerprint(
        Map<String, Object> inputMap,
        List<String> fields,
        List<Object> expectedValues,
        String expectedFingerprint
    ) throws Exception {
        return doTestFingerprint(inputMap, fields, expectedValues, expectedFingerprint, false, null);
    }

    private String doTestFingerprint(
        Map<String, Object> inputMap,
        List<String> fields,
        List<Object> expectedValues,
        String expectedFingerprint,
        boolean ignoreMissing
    ) throws Exception {
        return doTestFingerprint(inputMap, fields, expectedValues, expectedFingerprint, ignoreMissing, null);
    }

    private String doTestFingerprint(
        Map<String, Object> inputMap,
        List<String> fields,
        List<Object> expectedValues,
        String expectedFingerprint,
        boolean ignoreMissing,
        String salt
    ) throws Exception {
        FingerprintProcessor.Factory factory = new FingerprintProcessor.Factory();
        var config = new HashMap<String, Object>();
        config.put("fields", fields);
        config.put("ignore_missing", ignoreMissing);
        if (salt != null) {
            config.put("salt", salt);
        }
        FingerprintProcessor fp = factory.create(null, randomAlphaOfLength(10), null, config);

        byte[] expectedBytes = new byte[0];
        if (salt != null) {
            expectedBytes = toBytes(salt);
        }
        for (Object value : expectedValues) {
            expectedBytes = concatBytes(expectedBytes, DELIMITER);
            expectedBytes = concatBytes(expectedBytes, toBytes(value));
        }
        MessageDigest md = MessageDigest.getInstance(FingerprintProcessor.Factory.DEFAULT_METHOD);
        expectedBytes = md.digest(expectedBytes);

        var input = new IngestDocument(inputMap, Map.of());
        var output = fp.execute(input);
        assertTrue(output.hasField("fingerprint"));
        String fingerprint = output.getFieldValue("fingerprint", String.class);
        assertThat(fingerprint, equalTo(Base64.getEncoder().encodeToString(expectedBytes)));
        if (expectedFingerprint != null) {
            assertThat(fingerprint, equalTo(expectedFingerprint));
        }
        return fingerprint;
    }

    public void testMethod() throws Exception {
        var expectedFingerprints = List.of(
            "b+3QyaPYdnUF1lb5IKE+1g==",
            "SX/93t223OurJvgMUOCtSl9hcpg=",
            "zDQYTy34tBlmNedlDdn++N7NN+wBY15mCoPDINmUxXc=",
            "xNIpYyJzRmg5R0T44ZORC2tgh8N4tVtTFzD5AdBqxmdOuRUjibQQ64lgefkbuZFl8Hv9ze9U6PAmrlgJPcRPGA==",
            "yjfaOoy2UQ3EHZRAzFK9sw=="
        );

        var inputMap = new LinkedHashMap<String, Object>();
        inputMap.put("foo", "foo");
        inputMap.put("bar", "bar");
        FingerprintProcessor.Factory factory = new FingerprintProcessor.Factory();
        for (int k = 0; k < FingerprintProcessor.Factory.SUPPORTED_DIGESTS.length; k++) {
            var config = new HashMap<String, Object>();
            config.put("fields", List.of("foo", "bar"));
            config.put("method", FingerprintProcessor.Factory.SUPPORTED_DIGESTS[k]);

            FingerprintProcessor fp = factory.create(null, randomAlphaOfLength(10), null, config);
            var input = new IngestDocument(inputMap, Map.of());
            var output = fp.execute(input);
            assertTrue(output.hasField("fingerprint"));
            String fingerprint = output.getFieldValue("fingerprint", String.class);
            assertThat(fingerprint, equalTo(expectedFingerprints.get(k)));
        }
    }

    public void testBasicObjectTraversal() throws Exception {
        var fields = new ArrayList<String>();
        fields.add("foo");
        fields.add("bar");

        var inputMap = new HashMap<String, Object>();
        inputMap.put("foo", "foo1");
        inputMap.put("bar", "bar1");
        doTestObjectTraversal(inputMap, fields, List.of("bar1", "foo1"));
    }

    public void testObjectTraversalWithLists() throws Exception {
        var fields = new ArrayList<String>();
        fields.add("foo");
        fields.add("bar");

        var listInList = new ArrayList<>();
        listInList.add("rat");
        listInList.add("tiger");
        listInList.add("bear");

        var setInList = new LinkedHashSet<>();
        setInList.add("dog");
        setInList.add("cat");
        setInList.add("eel");

        var list = new ArrayList<>();
        list.add("zoo");
        list.add("yak");
        list.add(listInList);
        list.add(setInList);
        list.add("xor");

        var inputMap = new LinkedHashMap<String, Object>();
        inputMap.put("foo", list);
        inputMap.put("bar", "barValue");

        List<Object> expectedValues = List.of("barValue", "zoo", "yak", "rat", "tiger", "bear", "cat", "dog", "eel", "xor");

        doTestObjectTraversal(inputMap, fields, expectedValues);
    }

    public void testObjectTraversalWithMaps() throws Exception {
        var fields = new ArrayList<String>();
        fields.add("foo");
        fields.add("bar");

        var fooSubMap = new LinkedHashMap<String, Object>();
        fooSubMap.put("foo-sub1", "foo3");
        fooSubMap.put("foo-sub2", "foo2");
        var barSubMap = new LinkedHashMap<String, Object>();
        barSubMap.put("bar-sub1", "bar3");
        barSubMap.put("bar-sub2", "bar2");
        var inputMap = new LinkedHashMap<String, Object>();
        inputMap.put("foo", fooSubMap);
        inputMap.put("bar", barSubMap);

        List<Object> expectedValues = List.of("bar-sub1", "bar3", "bar-sub2", "bar2", "foo-sub1", "foo3", "foo-sub2", "foo2");

        doTestObjectTraversal(inputMap, fields, expectedValues);
    }

    public void testObjectTraversalWithSets() throws Exception {
        var fields = new ArrayList<String>();
        fields.add("foo");
        fields.add("bar");

        var fooSet = new LinkedHashSet<String>();
        fooSet.add("foo3");
        fooSet.add("foo2");
        var barSet = new LinkedHashSet<String>();
        barSet.add("bar3");
        barSet.add("bar2");
        var inputMap = new LinkedHashMap<String, Object>();
        inputMap.put("foo", fooSet);
        inputMap.put("bar", barSet);

        List<Object> expectedValues = List.of("bar2", "bar3", "foo2", "foo3");

        doTestObjectTraversal(inputMap, fields, expectedValues);
    }

    public void testObjectTraversalWithNestedStructures() throws Exception {
        var fields = new ArrayList<String>();
        fields.add("foo");
        fields.add("bar");

        var mapInList = new LinkedHashMap<String, Object>();
        mapInList.put("abc", "def");
        mapInList.put("ghi", "jkl");

        ZonedDateTime now = ZonedDateTime.now();
        List<Object> listInMap = new ArrayList<>();
        listInMap.add(now);
        listInMap.add("foo");
        listInMap.add(mapInList);
        listInMap.add(3.14D);

        var fooMap = new LinkedHashMap<String, Object>();
        fooMap.put("list", listInMap);
        fooMap.put("alpha", "beta");

        var inputMap = new LinkedHashMap<String, Object>();
        inputMap.put("foo", fooMap);
        inputMap.put("bar", "barValue");

        List<Object> expectedValues = List.of("barValue", "alpha", "beta", "list", now, "foo", "abc", "def", "ghi", "jkl", 3.14D);

        doTestObjectTraversal(inputMap, fields, expectedValues);
    }

    private void doTestObjectTraversal(Map<String, Object> inputMap, List<String> fields, List<Object> expectedValues) throws Exception {
        ThreadLocal<FingerprintProcessor.Hasher> threadLocalHasher = ThreadLocal.withInitial(TestHasher::new);
        FingerprintProcessor fp = new FingerprintProcessor(
            FingerprintProcessor.TYPE,
            "",
            fields,
            "fingerprint",
            new byte[0],
            threadLocalHasher,
            false
        );

        byte[] expectedBytes = new byte[0];
        for (Object value : expectedValues) {
            expectedBytes = concatBytes(expectedBytes, DELIMITER);
            expectedBytes = concatBytes(expectedBytes, toBytes(value));
        }

        var input = new IngestDocument(inputMap, Map.of());
        var output = fp.execute(input);
        var hasher = (TestHasher) threadLocalHasher.get();
        assertThat(hasher.getBytesSeen(), equalTo(expectedBytes));
        assertTrue(output.hasField("fingerprint"));
        assertThat(output.getFieldValue("fingerprint", String.class), equalTo(Base64.getEncoder().encodeToString(expectedBytes)));
    }

    static byte[] concatBytes(byte[] bytes1, byte[] bytes2) {
        byte[] newBytes = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, newBytes, 0, bytes1.length);
        System.arraycopy(bytes2, 0, newBytes, bytes1.length, bytes2.length);
        return newBytes;
    }

    static class TestHasher implements FingerprintProcessor.Hasher {

        private byte[] bytesSeen = new byte[0];

        @Override
        public void reset() {
            bytesSeen = new byte[0];
        }

        @Override
        public void update(byte[] input) {
            this.bytesSeen = concatBytes(bytesSeen, input);
        }

        @Override
        public byte[] digest() {
            // doesn't reset so that the bytes seen can be verified
            return bytesSeen;
        }

        public byte[] getBytesSeen() {
            return bytesSeen;
        }

        public String getAlgorithm() {
            return "test";
        }
    }
}
