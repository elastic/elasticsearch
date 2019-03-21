/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;
import static org.hamcrest.Matchers.is;

public class FieldHitExtractorTests extends AbstractWireSerializingTestCase<FieldHitExtractor> {

    public static FieldHitExtractor randomFieldHitExtractor() {
        String hitName = randomAlphaOfLength(5);
        String name = randomAlphaOfLength(5) + "." + hitName;
        return new FieldHitExtractor(name, null, randomZone(), randomBoolean(), hitName, false);
    }

    @Override
    protected FieldHitExtractor createTestInstance() {
        return randomFieldHitExtractor();
    }

    @Override
    protected Reader<FieldHitExtractor> instanceReader() {
        return FieldHitExtractor::new;
    }

    @Override
    protected FieldHitExtractor mutateInstance(FieldHitExtractor instance) {
        return new FieldHitExtractor(
            instance.fieldName() + "mutated",
            randomValueOtherThan(instance.dataType(), () -> randomFrom(DataType.values())),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone),
            randomBoolean(),
            instance.hitName() + "mutated",
            randomBoolean());
    }

    public void testGetDottedValueWithDocValues() {
        String grandparent = randomAlphaOfLength(5);
        String parent = randomAlphaOfLength(5);
        String child = randomAlphaOfLength(5);
        String fieldName = grandparent + "." + parent + "." + child;

        FieldHitExtractor extractor = getFieldHitExtractor(fieldName, true);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {

            List<Object> documentFieldValues = new ArrayList<>();
            if (randomBoolean()) {
                documentFieldValues.add(randomValue());
            }

            SearchHit hit = new SearchHit(1);
            DocumentField field = new DocumentField(fieldName, documentFieldValues);
            hit.fields(singletonMap(fieldName, field));
            Object result = documentFieldValues.isEmpty() ? null : documentFieldValues.get(0);
            assertEquals(result, extractor.extract(hit));
        }
    }

    public void testGetDottedValueWithSource() throws Exception {
        String grandparent = randomAlphaOfLength(5);
        String parent = randomAlphaOfLength(5);
        String child = randomAlphaOfLength(5);
        String fieldName = grandparent + "." + parent + "." + child;

        FieldHitExtractor extractor = getFieldHitExtractor(fieldName, false);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            /* We use values that are parsed from json as "equal" to make the
             * test simpler. */
            Object value = randomValue();
            SearchHit hit = new SearchHit(1);
            XContentBuilder source = JsonXContent.contentBuilder();
            boolean hasGrandparent = randomBoolean();
            boolean hasParent = randomBoolean();
            boolean hasChild = randomBoolean();
            boolean hasSource = hasGrandparent && hasParent && hasChild;

            source.startObject();
            if (hasGrandparent) {
                source.startObject(grandparent);
                if (hasParent) {
                    source.startObject(parent);
                    if (hasChild) {
                        source.field(child, value);
                        if (randomBoolean()) {
                            source.field(fieldName + randomAlphaOfLength(3), value + randomAlphaOfLength(3));
                        }
                    }
                    source.endObject();
                }
                source.endObject();
            }
            source.endObject();
            BytesReference sourceRef = BytesReference.bytes(source);
            hit.sourceRef(sourceRef);
            Object extract = extractor.extract(hit);
            assertEquals(hasSource ? value : null, extract);
        }
    }

    public void testGetDocValue() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor extractor = getFieldHitExtractor(fieldName, true);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            List<Object> documentFieldValues = new ArrayList<>();
            if (randomBoolean()) {
                documentFieldValues.add(randomValue());
            }
            SearchHit hit = new SearchHit(1);
            DocumentField field = new DocumentField(fieldName, documentFieldValues);
            hit.fields(singletonMap(fieldName, field));
            Object result = documentFieldValues.isEmpty() ? null : documentFieldValues.get(0);
            assertEquals(result, extractor.extract(hit));
        }
    }

    public void testGetDate() {
        ZoneId zoneId = randomZone();
        long millis = 1526467911780L;
        List<Object> documentFieldValues = Collections.singletonList(Long.toString(millis));
        SearchHit hit = new SearchHit(1);
        DocumentField field = new DocumentField("my_date_field", documentFieldValues);
        hit.fields(singletonMap("my_date_field", field));
        FieldHitExtractor extractor = new FieldHitExtractor("my_date_field", DataType.DATETIME, zoneId, true);
        assertEquals(DateUtils.asDateTime(millis, zoneId), extractor.extract(hit));
    }

    public void testGetSource() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor extractor = getFieldHitExtractor(fieldName, false);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            /* We use values that are parsed from json as "equal" to make the
             * test simpler. */
            Object value = randomValue();
            SearchHit hit = new SearchHit(1);
            XContentBuilder source = JsonXContent.contentBuilder();
            source.startObject(); {
                source.field(fieldName, value);
                if (randomBoolean()) {
                    source.field(fieldName + "_random_junk", value + "_random_junk");
                }
            }
            source.endObject();
            BytesReference sourceRef = BytesReference.bytes(source);
            hit.sourceRef(sourceRef);
            assertEquals(value, extractor.extract(hit));
        }
    }

    public void testToString() {
        assertEquals("hit.field@hit@Europe/Berlin",
            new FieldHitExtractor("hit.field", null, ZoneId.of("Europe/Berlin"), true, "hit", false).toString());
    }

    public void testMultiValuedDocValue() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = getFieldHitExtractor(fieldName, true);
        SearchHit hit = new SearchHit(1);
        DocumentField field = new DocumentField(fieldName, asList("a", "b"));
        hit.fields(singletonMap(fieldName, field));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Arrays (returned by [" + fieldName + "]) are not supported"));
    }

    public void testMultiValuedSourceValue() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = getFieldHitExtractor(fieldName, false);
        SearchHit hit = new SearchHit(1);
        XContentBuilder source = JsonXContent.contentBuilder();
        source.startObject(); {
            source.field(fieldName, asList("a", "b"));
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);
        SqlException ex = expectThrows(SqlException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Arrays (returned by [" + fieldName + "]) are not supported"));
    }

    public void testSingleValueArrayInSource() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = getFieldHitExtractor(fieldName, false);
        SearchHit hit = new SearchHit(1);
        XContentBuilder source = JsonXContent.contentBuilder();
        Object value = randomValue();
        source.startObject(); {
            source.field(fieldName, Collections.singletonList(value));
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);
        assertEquals(value, fe.extract(hit));
    }

    public void testExtractSourcePath() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c", false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b", singletonMap("c", value)));
        assertThat(fe.extractFromSource(map), is(value));
    }

    public void testExtractSourceIncorrectPath() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c.d", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b", singletonMap("c", value)));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot extract value [a.b.c.d] from source"));
    }

    public void testMultiValuedSource() {
        FieldHitExtractor fe = getFieldHitExtractor("a", false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", asList(value, value));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Arrays (returned by [a]) are not supported"));
    }

    public void testMultiValuedSourceAllowed() {
        FieldHitExtractor fe = new FieldHitExtractor("a", null, UTC, false, true);
        Object valueA = randomValue();
        Object valueB = randomValue();
        Map<String, Object> map = singletonMap("a", asList(valueA, valueB));
        assertEquals(valueA, fe.extractFromSource(map));
    }

    public void testFieldWithDots() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b", false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a.b", value);
        assertEquals(value, fe.extractFromSource(map));
    }

    public void testNestedFieldWithDots() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c", false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b.c", value));
        assertEquals(value, fe.extractFromSource(map));
    }

    public void testNestedFieldWithDotsWithNestedField() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c.d", false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b.c", singletonMap("d", value)));
        assertEquals(value, fe.extractFromSource(map));
    }

    public void testNestedFieldWithDotsWithNestedFieldWithDots() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c.d.e", false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b.c", singletonMap("d.e", value)));
        assertEquals(value, fe.extractFromSource(map));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testNestedFieldsWithDotsAndRandomHiearachy() {
        String[] path = new String[100];
        StringJoiner sj = new StringJoiner(".");
        for (int i = 0; i < 100; i++) {
            path[i] = randomAlphaOfLength(randomIntBetween(1, 10));
            sj.add(path[i]);
        }
        boolean arrayLeniency = randomBoolean();
        FieldHitExtractor fe = new FieldHitExtractor(sj.toString(), null, UTC, false, arrayLeniency);

        List<String> paths = new ArrayList<>(path.length);
        int start = 0;
        while (start < path.length) {
            int end = randomIntBetween(start + 1, path.length);
            sj = new StringJoiner(".");
            for (int j = start; j < end; j++) {
                sj.add(path[j]);
            }
            paths.add(sj.toString());
            start = end;
        }

        /*
         * Randomize how many values the field to look for will have (1 - 3). It's not really relevant how many values there are in the list
         * but that the list has one element or more than one.
         * If it has one value, then randomize the way it's indexed: as a single-value array or not e.g.: "a":"value" or "a":["value"].
         * If it has more than one value, it will always be an array e.g.: "a":["v1","v2","v3"].
         */
        int valuesCount = randomIntBetween(1, 3);
        Object value = randomValue();
        if (valuesCount == 1) {
            value = randomBoolean() ? singletonList(value) : value;
        } else {
            value = new ArrayList(valuesCount);
            for(int i = 0; i < valuesCount; i++) {
                ((List) value).add(randomValue());
            }
        }

        // the path to the randomly generated fields path
        StringBuilder expected = new StringBuilder(paths.get(paths.size() - 1));
        // the actual value we will be looking for in the test at the end
        Map<String, Object> map = singletonMap(paths.get(paths.size() - 1), value);
        // build the rest of the path and the expected path to check against in the error message
        for (int i = paths.size() - 2; i >= 0; i--) {
            map = singletonMap(paths.get(i), randomBoolean() ? singletonList(map) : map);
            expected.insert(0, paths.get(i) + ".");
        }

        if (valuesCount == 1 || arrayLeniency) {
            // if the number of generated values is 1, just check we return the correct value
            assertEquals(value instanceof List ? ((List) value).get(0) : value, fe.extractFromSource(map));
        } else {
            // if we have an array with more than one value in it, check that we throw the correct exception and exception message
            final Map<String, Object> map2 = Collections.unmodifiableMap(map);
            SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map2));
            assertThat(ex.getMessage(), is("Arrays (returned by [" + expected + "]) are not supported"));
        }
    }

    public void testExtractSourceIncorrectPathWithFieldWithDots() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c.d.e", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b.c", singletonMap("d", value)));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot extract value [a.b.c.d.e] from source"));
    }

    public void testFieldWithDotsAndCommonPrefix() {
        FieldHitExtractor fe1 = getFieldHitExtractor("a.d", false);
        FieldHitExtractor fe2 = getFieldHitExtractor("a.b.c", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = new HashMap<>();
        map.put("a", singletonMap("d", value));
        map.put("a.b", singletonMap("c", value));
        assertEquals(value, fe1.extractFromSource(map));
        assertEquals(value, fe2.extractFromSource(map));
    }

    public void testFieldWithDotsAndCommonPrefixes() {
        FieldHitExtractor fe1 = getFieldHitExtractor("a1.b.c.d1.e.f.g1", false);
        FieldHitExtractor fe2 = getFieldHitExtractor("a2.b.c.d2.e.f.g2", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = new HashMap<>();
        map.put("a1", singletonMap("b.c", singletonMap("d1", singletonMap("e.f", singletonMap("g1", value)))));
        map.put("a2", singletonMap("b.c", singletonMap("d2", singletonMap("e.f", singletonMap("g2", value)))));
        assertEquals(value, fe1.extractFromSource(map));
        assertEquals(value, fe2.extractFromSource(map));
    }

    public void testFieldWithDotsAndSamePathButDifferentHierarchy() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c.d.e.f.g", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = new HashMap<>();
        map.put("a.b", singletonMap("c", singletonMap("d.e", singletonMap("f.g", value))));
        map.put("a", singletonMap("b.c", singletonMap("d.e", singletonMap("f", singletonMap("g", value)))));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Multiple values (returned by [a.b.c.d.e.f.g]) are not supported"));
    }
    
    public void testFieldsWithSingleValueArrayAsSubfield() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = new HashMap<>();
        // "a" : [{"b" : "value"}]
        map.put("a", singletonList(singletonMap("b", value)));
        assertEquals(value, fe.extractFromSource(map));
    }
    
    public void testFieldsWithMultiValueArrayAsSubfield() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b", false);
        Map<String, Object> map = new HashMap<>();
        // "a" : [{"b" : "value1"}, {"b" : "value2"}]
        map.put("a", asList(singletonMap("b", randomNonNullValue()), singletonMap("b", randomNonNullValue())));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Arrays (returned by [a.b]) are not supported"));
    }
    
    public void testFieldsWithSingleValueArrayAsSubfield_TwoNestedLists() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = new HashMap<>();
        // "a" : [{"b" : [{"c" : "value"}]}]
        map.put("a", singletonList(singletonMap("b", singletonList(singletonMap("c", value)))));
        assertEquals(value, fe.extractFromSource(map));
    }
    
    public void testFieldsWithMultiValueArrayAsSubfield_ThreeNestedLists() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c", false);
        Map<String, Object> map = new HashMap<>();
        // "a" : [{"b" : [{"c" : ["value1", "value2"]}]}]
        map.put("a", singletonList(singletonMap("b", singletonList(singletonMap("c", asList("value1", "value2"))))));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Arrays (returned by [a.b.c]) are not supported"));
    }
    
    public void testFieldsWithSingleValueArrayAsSubfield_TwoNestedLists2() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = new HashMap<>();
        // "a" : [{"b" : {"c" : ["value"]}]}]
        map.put("a", singletonList(singletonMap("b", singletonMap("c", singletonList(value)))));
        assertEquals(value, fe.extractFromSource(map));
    }

    public void testObjectsForSourceValue() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = getFieldHitExtractor(fieldName, false);
        SearchHit hit = new SearchHit(1);
        XContentBuilder source = JsonXContent.contentBuilder();
        source.startObject(); {
            source.startObject(fieldName); {
                source.field("b", "c");
            }
            source.endObject();
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);
        SqlException ex = expectThrows(SqlException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Objects (returned by [" + fieldName + "]) are not supported"));
    }

    private FieldHitExtractor getFieldHitExtractor(String fieldName, boolean useDocValue) {
        return new FieldHitExtractor(fieldName, null, UTC, useDocValue);
    }

    private Object randomValue() {
        Supplier<Object> value = randomFrom(Arrays.asList(
                () -> randomAlphaOfLength(10),
                ESTestCase::randomLong,
                ESTestCase::randomDouble,
                () -> null));
        return value.get();
    }

    private Object randomNonNullValue() {
        Supplier<Object> value = randomFrom(Arrays.asList(
                () -> randomAlphaOfLength(10),
                ESTestCase::randomLong,
                ESTestCase::randomDouble));
        return value.get();
    }
}
