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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueHandling;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;

import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueHandling.EXTRACT_ARRAY;
import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueHandling.FAIL_IF_MULTIVALUE;
import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueHandling.EXTRACT_ONE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.SHAPE;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;
import static org.hamcrest.Matchers.is;

public class FieldHitExtractorTests extends AbstractSqlWireSerializingTestCase<FieldHitExtractor> {

    public static FieldHitExtractor randomFieldHitExtractor() {
        String hitName = randomAlphaOfLength(5);
        String name = randomAlphaOfLength(5) + "." + hitName;
        return new FieldHitExtractor(name, null, null, randomZone(), randomBoolean(), hitName, FAIL_IF_MULTIVALUE);
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
    protected ZoneId instanceZoneId(FieldHitExtractor instance) {
        return instance.zoneId();
    }

    @Override
    protected FieldHitExtractor mutateInstance(FieldHitExtractor instance) {
        return new FieldHitExtractor(
            instance.fieldName() + "mutated",
            instance.fullFieldName() + "mutated",
            randomValueOtherThan(instance.dataType(), () -> randomFrom(SqlDataTypes.types())),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone),
            randomBoolean(),
            instance.hitName() + "mutated",
            randomFrom(MultiValueHandling.values()));
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

            DocumentField field = new DocumentField(fieldName, documentFieldValues);
            SearchHit hit = new SearchHit(1, null, singletonMap(fieldName, field), null);
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
            assertFieldHitEquals(hasSource ? value : null, extract);
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
            DocumentField field = new DocumentField(fieldName, documentFieldValues);
            SearchHit hit = new SearchHit(1, null, singletonMap(fieldName, field), null);
            Object result = documentFieldValues.isEmpty() ? null : documentFieldValues.get(0);
            assertEquals(result, extractor.extract(hit));
        }
    }

    public void testGetDate() {
        ZoneId zoneId = randomZone();
        long millis = 1526467911780L;
        List<Object> documentFieldValues = Collections.singletonList(Long.toString(millis));
        DocumentField field = new DocumentField("my_date_field", documentFieldValues);
        SearchHit hit = new SearchHit(1, null, singletonMap("my_date_field", field), null);
        FieldHitExtractor extractor = new FieldHitExtractor("my_date_field", DATETIME, zoneId, true);
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
            assertFieldHitEquals(value, extractor.extract(hit));
        }
    }

    public void testToString() {
        assertEquals("hit.field@hit@Europe/Berlin",
                new FieldHitExtractor("hit.field", null, null, ZoneId.of("Europe/Berlin"), true, "hit", FAIL_IF_MULTIVALUE)
                    .toString());
    }

    public void testMultiValuedDocValue() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = getFieldHitExtractor(fieldName, true);
        DocumentField field = new DocumentField(fieldName, asList("a", "b"));
        SearchHit hit = new SearchHit(1, null, singletonMap(fieldName, field), null);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [" + fieldName + "]; " +
            "use ARRAY(" + fieldName + ") instead"));
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
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [" + fieldName + "]; " +
            "use ARRAY(" + fieldName + ") instead"));
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
        assertFieldHitEquals(value, fe.extract(hit));
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
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot extract value [a.b.c.d] from source"));
    }

    public void testMultiValuedSource() {
        FieldHitExtractor fe = getFieldHitExtractor("a", false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", asList(value, value));
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [a]; use ARRAY(a) instead"));
    }

    public void testMultiValuedSourceAllowed() {
        FieldHitExtractor fe = new FieldHitExtractor("a", null, UTC, false, EXTRACT_ONE);
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
    public void testNestedFieldsWithDotsAndRandomHierarchy() {
        String[] path = new String[100];
        StringJoiner sj = new StringJoiner(".");
        for (int i = 0; i < 100; i++) {
            path[i] = randomAlphaOfLength(randomIntBetween(1, 10));
            sj.add(path[i]);
        }
        MultiValueHandling extractionMode = randomFrom(FAIL_IF_MULTIVALUE, EXTRACT_ONE, EXTRACT_ARRAY);
        FieldHitExtractor fe = new FieldHitExtractor(sj.toString(), null, UTC, false, extractionMode);

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

        List expectList = value instanceof List ? (List) value : (value == null ? emptyList() : singletonList(value));
        Object expectSingleton = value instanceof List ? ((List) value).get(0) : value;
        if (valuesCount == 1) {
            // if the number of generated values is 1, just check we return the correct value
            assertEquals(extractionMode == EXTRACT_ARRAY ? expectList : expectSingleton, fe.extractFromSource(map));
        } else {
            switch (extractionMode) {
                case FAIL_IF_MULTIVALUE:
                    // if we have an array with more than one value in it, check that we throw the correct exception and exception message
                    final Map<String, Object> map2 = map;
                    QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map2));
                    assertThat(ex.getMessage(), is("Cannot return multiple values for field [" + expected+ "]; " +
                        "use ARRAY(" + expected + ") instead"));
                    break;
                case EXTRACT_ONE:
                    assertEquals(expectSingleton, fe.extractFromSource(map));
                    break;
                case EXTRACT_ARRAY:
                    assertEquals(expectList, fe.extractFromSource(map));
                    break;
                default:
                    fail();
            }
        }
    }

    public void testExtractSourceIncorrectPathWithFieldWithDots() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c.d.e", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b.c", singletonMap("d", value)));
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map));
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
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [a.b.c.d.e.f.g]; use ARRAY(a.b.c.d.e.f.g) instead"));
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
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [a.b]; use ARRAY(a.b) instead"));
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
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [a.b.c]; use ARRAY(a.b.c) instead"));
    }

    public void testFieldsWithSingleValueArrayAsSubfield_TwoNestedLists2() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c", false);
        Object value = randomNonNullValue();
        Map<String, Object> map = new HashMap<>();
        // "a" : [{"b" : {"c" : ["value"]}]}]
        map.put("a", singletonList(singletonMap("b", singletonMap("c", singletonList(value)))));
        assertEquals(value, fe.extractFromSource(map));
    }

    // "a": null
    public void testMultiValueNull() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", null);
        FieldHitExtractor fea = getArrayFieldHitExtractor("a", INTEGER);
        assertEquals(singletonList(null), fea.extractFromSource(map));
    }

    // "a": [] / missing
    public void testMultiValueEmpty() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", emptyList());
        FieldHitExtractor fea = getArrayFieldHitExtractor("a", INTEGER);
        assertEquals(emptyList(), fea.extractFromSource(map));

        FieldHitExtractor feb = getArrayFieldHitExtractor("b", INTEGER);
        assertEquals(emptyList(), feb.extractFromSource(map));
    }

    // "a": [i1, i2, ..] as Map
    public void testMultiValueImmediateFromMap() {
        String fieldName = randomAlphaOfLength(5);
        Map<String, Object> map = new HashMap<>();
        List<Integer> list = randomList(2, 10, ESTestCase::randomInt);
        map.put(fieldName, list);
        FieldHitExtractor fe = getArrayFieldHitExtractor(fieldName, INTEGER);
        assertEquals(list, fe.extractFromSource(map));
    }

    // "a": [i1, i2, ..] XContentBuilder-serialized
    public void testMultiValueImmediateFromHit() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        List<Long> list = randomList(2, 10, ESTestCase::randomLong);

        SearchHit hit = new SearchHit(1);
        XContentBuilder source = JsonXContent.contentBuilder();
        source.startObject(); {
            source.startArray(fieldName); {
                list.forEach(x -> {
                    try {
                        source.value(x);
                    } catch (IOException ignored) {
                        fail();
                    }
                });
            }
            source.endArray();
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);

        FieldHitExtractor fe = getArrayFieldHitExtractor(fieldName, LONG);
        assertEquals(list, fe.extract(hit));
    }

    // {"a": {"b": {"c": 1}}, "a": {"b.c": [2, 3]}, "a.b": [{"c": 4}, {"c": 5}], ...}
    @SuppressWarnings("unchecked")
    public void testMultiValueDifferentPathsWithArraysAndNulls() {
        int depth = randomIntBetween(3, 10); // depth excluding the leaf
        List<String> nodes = randomList(2, depth, () -> randomAlphaOfLength(randomIntBetween(1, 5)));
        List<List<String>> paths = generatePaths(nodes);

        List<Double> valuesList = new ArrayList<>(paths.size());
        Map<String, Object> map = new HashMap<>();
        for (List<String> path : paths) {
            Object value;
            Supplier<Double> supplier = () -> randomNonNegativeByte() < 30 ? null : randomDouble();
            if (randomBoolean()) {
                value = supplier.get();
                valuesList.add((Double) value);
            } else {
                value = randomList(1, 5, supplier);
                valuesList.addAll((List<Double>) value);
            }
            if (path.size() == 1) { // "a.b.c": 3
                map.put(path.get(0), value);
            } else {
                Map<String, Object> crrMap = new HashMap<>();
                crrMap.put(path.get(path.size() - 1), value);
                for (int j = path.size() - 2; j > 0; j--) {
                    Map<String, Object> newMap = new HashMap<>();
                    newMap.put(path.get(j), crrMap);
                    crrMap = newMap;
                }
                mergeMaps(map, path.get(0), crrMap);
            }
        }
        randomlyMultiplicateSubmaps(map, valuesList);

        String fieldName = join(".", paths.get(0));
        FieldHitExtractor fe = getArrayFieldHitExtractor(fieldName, DOUBLE);

        Object result = fe.extractFromSource(map);
        assertTrue(result instanceof List);
        List<Double> resultList = (List<Double>) result;

        // order of array retrieval is stable, not trivial to predict and ultimately irrelevant / not guaranteed
        valuesList.sort(Comparator.nullsLast(Double::compare));
        resultList.sort(Comparator.nullsLast(Double::compare));
        assertEquals(valuesList, resultList);
    }

    // "a": {"b": 2} => "a": [{"b": 2}, {"b": 2}]
    private static void randomlyMultiplicateSubmaps(Map<String, Object> map, List<Double> valuesList) {
        map.keySet().forEach(key -> {
            Object val = map.get(key);
            if (val instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> innerMap = (Map<String, Object>) val;
                if (randomNonNegativeByte() < 60) {
                    List<Map<String, Object>> replacementList = new ArrayList<>();
                    int multiplicate = randomIntBetween(1, 5);
                    for (int i = 0; i < multiplicate; i++) {
                        Map<String, Object> copy = new HashMap<>(innerMap);
                        replacementList.add(copy);
                        if (i > 0) { // the initial copy is already part of valuesList
                            collectLeaves(copy, valuesList);
                        }
                    }
                    map.put(key, replacementList);
                } else {
                    randomlyMultiplicateSubmaps(innerMap, valuesList);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static void collectLeaves(Map<String, Object> map, List<Double> valuesList) {
        for (Object val : map.values()) {
            if (val instanceof Map) {
                collectLeaves((Map<String, Object>) val, valuesList);
            } else if (val instanceof List) {
                for (Object o : (List<Object>) val) {
                    if (o instanceof Map) {
                        collectLeaves((Map<String, Object>) o, valuesList);
                    } else {
                        valuesList.add((Double) o);
                    }
                }
            } else {
                valuesList.add((Double) val);
            }
        }
    }

    // {"a" : {"b": {"c": 3}}} + "a", {"b.c": 4}  => {"a": {"b": {"c": 3}, "b.c": 4}}
    @SuppressWarnings("unchecked")
    private static void mergeMaps(Map<String, Object> destination, String key, Map<String, Object> singleKeys) {
        Object o = singleKeys;
        while (destination.containsKey(key)) {
            destination = (Map<String, Object>) destination.get(key);
            key = singleKeys.keySet().toArray(new String[0])[0];
            o = singleKeys.get(key);
            if (o instanceof Map == false) {
                break;
            }
            singleKeys = (Map<String, Object>) o;
        }
        destination.put(key, o);
    }

    // generate all possible path combinations with given node names: a, b, c => (a, b, c), (a, b.c), (a.b, c), (a.b.c)
    private static List<List<String>> generatePaths(List<String> nodes) {
        if (nodes.size() == 0) {
            return emptyList();
        }
        List<List<String>> paths = new ArrayList<>(singletonList(singletonList(nodes.get(0))));
        for (int i = 1; i < nodes.size(); i++) {
            List<List<String>> newPaths = new ArrayList<>();
            for (List<String> crrPath : paths) {
                newPaths.addAll(extendPaths(crrPath, nodes.get(i)));
            }
            paths = newPaths;
        }
        return paths;
    }

    // (a, b) + c => (a, b, c), (a, bc)
    private static List<List<String>> extendPaths(List<String> paths, String node) {
        List<List<String>> extendedPaths = new ArrayList<>(paths.size() * 2);
        List<String> listA = new ArrayList<>(paths);
        listA.add(node);
        extendedPaths.add(listA);
        if (paths.isEmpty() == false) {
            List<String> listB;
            if (paths.size() > 1) {
                listB = paths.subList(0, paths.size() - 1);
                listB.add(paths.get(paths.size() - 1) + "." + node);
            } else {
                listB = singletonList(paths.get(0) + "." + node);
            }
            extendedPaths.add(listB);
        }
        return extendedPaths;
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
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Objects (returned by [" + fieldName + "]) are not supported"));
    }

    public void testMultipleObjectsForSourceValue() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = randomBoolean()
            ? getFieldHitExtractor(fieldName, false)
            : getArrayFieldHitExtractor(fieldName, randomFrom(SqlDataTypes.types()));
        SearchHit hit = new SearchHit(1);
        int arraySize = randomIntBetween(1, 4);
        XContentBuilder source = JsonXContent.contentBuilder();
        source.startObject(); {
            source.startArray(fieldName); {
                for (int i = 0; i < arraySize; i++) {
                    source.startObject(); {
                        source.field("b" + i, "c");
                    }
                    source.endObject();
                }
            }
            source.endArray();
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Objects (returned by [" + fieldName + "]) are not supported"));
    }

    public void testGeoShapeExtraction() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, randomBoolean() ? GEO_SHAPE : SHAPE, UTC, false);
        Map<String, Object> map = new HashMap<>();
        map.put(fieldName, "POINT (1 2)");
        assertEquals(new GeoShape(1, 2), fe.extractFromSource(map));

        map = new HashMap<>();
        assertNull(fe.extractFromSource(map));
    }


    public void testMultipleGeoShapeExtraction() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, randomBoolean() ? GEO_SHAPE : SHAPE, UTC, false);
        Map<String, Object> map = new HashMap<>();
        map.put(fieldName, "POINT (1 2)");
        assertEquals(new GeoShape(1, 2), fe.extractFromSource(map));

        map = new HashMap<>();
        assertNull(fe.extractFromSource(map));

        Map<String, Object> map2 = new HashMap<>();
        map2.put(fieldName, asList("POINT (1 2)", "POINT (3 4)"));
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extractFromSource(map2));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [" + fieldName + "]; " +
            "use ARRAY(" + fieldName + ") instead"));

        FieldHitExtractor lenientFe = new FieldHitExtractor(fieldName,
            randomBoolean() ? GEO_SHAPE : SHAPE, UTC, false, EXTRACT_ONE);
        assertEquals(new GeoShape(1, 2), lenientFe.extractFromSource(map2));

        FieldHitExtractor arrayFe = getArrayFieldHitExtractor(fieldName, randomBoolean() ? GEO_SHAPE : SHAPE);
        assertEquals(asList(new GeoShape(1, 2), new GeoShape(3, 4)), arrayFe.extractFromSource(map2));
    }

    public void testGeoPointExtractionFromSource() throws IOException {
        int layers = randomIntBetween(1, 3);
        String pathCombined = "";
        double lat = randomDoubleBetween(-90, 90, true);
        double lon = randomDoubleBetween(-180, 180, true);
        SearchHit hit = new SearchHit(1);
        XContentBuilder source = JsonXContent.contentBuilder();
        boolean[] arrayWrap = new boolean[layers - 1];
        source.startObject(); {
            for (int i = 0; i < layers - 1; i++) {
                arrayWrap[i] = randomBoolean();
                String name = randomAlphaOfLength(10);
                source.field(name);
                if (arrayWrap[i]) {
                    source.startArray();
                }
                source.startObject();
                pathCombined = pathCombined + name + ".";
            }
            String name = randomAlphaOfLength(10);
            pathCombined = pathCombined + name;
            source.field(name, randomSpecPoint(lat, lon));
            for (int i = layers - 2; i >= 0; i--) {
                source.endObject();
                if (arrayWrap[i]) {
                    source.endArray();
                }
            }
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);

        FieldHitExtractor fe = new FieldHitExtractor(pathCombined, GEO_POINT, UTC, false);
        assertEquals(new GeoShape(lon, lat), fe.extract(hit));
    }

    public void testMultipleGeoPointExtractionFromSource() throws IOException {
        SearchHit hit = new SearchHit(1);
        String fieldName = randomAlphaOfLength(5);
        int arraySize = randomIntBetween(2, 4);
        List<GeoShape> geoShapes = new ArrayList<>(arraySize);
        XContentBuilder source = JsonXContent.contentBuilder();
        source.startObject(); {
            source.startArray(fieldName);
            for (int i = 1; i <= arraySize; i++) {
                double lat = randomDoubleBetween(-90, 90, true);
                double lon = randomDoubleBetween(-180, 180, true);
                source.value(randomSpecPoint(lat, lon));
                geoShapes.add(new GeoShape(lon, lat));
            }
            source.endArray();
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);

        FieldHitExtractor fe = new FieldHitExtractor(fieldName, GEO_POINT, UTC, false);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [" + fieldName + "]; " +
            "use ARRAY(" + fieldName + ") instead"));

        FieldHitExtractor lenientFe = new FieldHitExtractor(fieldName, GEO_POINT, UTC, false, EXTRACT_ONE);
        assertEquals(geoShapes.get(0), lenientFe.extract(hit));

        FieldHitExtractor arrayFe = getArrayFieldHitExtractor(fieldName, GEO_POINT);
        assertEquals(geoShapes, arrayFe.extract(hit));
    }

    public void testGeoPointExtractionFromDocValues() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, GEO_POINT, UTC, true);
        DocumentField field = new DocumentField(fieldName, singletonList("2, 1"));
        SearchHit hit = new SearchHit(1, null, singletonMap(fieldName, field), null);
        assertEquals(new GeoShape(1, 2), fe.extract(hit));
        hit = new SearchHit(1);
        assertNull(fe.extract(hit));
    }

    public void testGeoPointExtractionFromMultipleDocValues() {
        String fieldName = randomAlphaOfLength(5);
        SearchHit hit = new SearchHit(1, null, singletonMap(fieldName,
            new DocumentField(fieldName, asList("2,1", "3,4"))), null);

        FieldHitExtractor fe = new FieldHitExtractor(fieldName, GEO_POINT, UTC, true);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Cannot return multiple values for field [" + fieldName + "]; " +
            "use ARRAY(" + fieldName + ") instead"));

        FieldHitExtractor lenientFe = new FieldHitExtractor(fieldName, GEO_POINT, UTC, true, EXTRACT_ONE);
        assertEquals(new GeoShape(1, 2), lenientFe.extract(hit));

        FieldHitExtractor arrayFe = new FieldHitExtractor(fieldName, GEO_POINT, UTC, true, EXTRACT_ARRAY);
        assertEquals(asList(new GeoShape(1, 2), new GeoShape(4, 3)), arrayFe.extract(hit));
    }



    private FieldHitExtractor getFieldHitExtractor(String fieldName, boolean useDocValue) {
        return new FieldHitExtractor(fieldName, null, UTC, useDocValue);
    }

    private static FieldHitExtractor getArrayFieldHitExtractor(String fieldName, DataType dataType) {
        return new FieldHitExtractor(fieldName, dataType, UTC, false, EXTRACT_ARRAY);
    }

    private Object randomValue() {
        Supplier<Object> value = randomFrom(asList(
                () -> randomAlphaOfLength(10),
                ESTestCase::randomLong,
                ESTestCase::randomDouble,
                ESTestCase::randomInt,
                () -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
                () -> new BigDecimal("20012312345621343256123456254.20012312345621343256123456254"),
                () -> null));
        return value.get();
    }

    private Object randomNonNullValue() {
        Supplier<Object> value = randomFrom(asList(
                () -> randomAlphaOfLength(10),
                ESTestCase::randomLong,
                ESTestCase::randomDouble,
                ESTestCase::randomInt,
                () -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
                () -> new BigDecimal("20012312345621343256123456254.20012312345621343256123456254")));
        return value.get();
    }

    private void assertFieldHitEquals(Object expected, Object actual) {
        if (expected instanceof BigDecimal) {
            // parsing will, by default, build a Double even if the initial value is BigDecimal
            // Elasticsearch does this the same when returning the results
            assertEquals(((BigDecimal) expected).doubleValue(), actual);
        } else {
            assertEquals(expected, actual);
        }
    }

    private Object randomSpecPoint(double lat, double lon) {
        Supplier<Object> value = randomFrom(asList(
            () -> lat + "," + lon,
            () -> asList(lon, lat),
            () -> {
                Map<String, Object> map1 = new HashMap<>();
                map1.put("lat", lat);
                map1.put("lon", lon);
                return map1;
            }
        ));
        return value.get();
    }
}
