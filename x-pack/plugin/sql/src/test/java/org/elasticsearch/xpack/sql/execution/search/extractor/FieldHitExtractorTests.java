/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.time.DateUtils.toMilliSeconds;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.SHAPE;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;
import static org.hamcrest.Matchers.is;

public class FieldHitExtractorTests extends AbstractSqlWireSerializingTestCase<FieldHitExtractor> {

    public static FieldHitExtractor randomFieldHitExtractor() {
        String hitName = randomAlphaOfLength(5);
        String name = randomAlphaOfLength(5) + "." + hitName;
        return new FieldHitExtractor(name, null, randomZone(), hitName, false);
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
            randomValueOtherThan(instance.dataType(), () -> randomFrom(SqlDataTypes.types())),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone),
            instance.hitName() + "mutated",
            randomBoolean()
        );
    }

    public void testGetDottedValueWithDocValues() {
        String grandparent = randomAlphaOfLength(5);
        String parent = randomAlphaOfLength(5);
        String child = randomAlphaOfLength(5);
        String fieldName = grandparent + "." + parent + "." + child;

        FieldHitExtractor extractor = getFieldHitExtractor(fieldName);

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

    public void testGetDocValue() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor extractor = getFieldHitExtractor(fieldName);

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
        long totalNanos = randomLongBetween(72000000000000L, Long.MAX_VALUE);
        long millis = toMilliSeconds(totalNanos);
        long nanosOnly = (int) (totalNanos % 1_000_000_000);
        ZonedDateTime zdt = DateUtils.asDateTimeWithMillis(millis, zoneId).plusNanos(nanosOnly);
        List<Object> documentFieldValues = Collections.singletonList(StringUtils.toString(zdt));
        DocumentField field = new DocumentField("my_date_nanos_field", documentFieldValues);
        SearchHit hit = new SearchHit(1, null, singletonMap("my_date_nanos_field", field), null);
        FieldHitExtractor extractor = new FieldHitExtractor("my_date_nanos_field", DATETIME, zoneId, true);
        assertEquals(zdt, extractor.extract(hit));
    }

    public void testToString() {
        assertEquals(
            "hit.field@hit@Europe/Berlin",
            new FieldHitExtractor("hit.field", null, ZoneId.of("Europe/Berlin"), "hit", false).toString()
        );
    }

    public void testMultiValuedDocValue() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = getFieldHitExtractor(fieldName);
        DocumentField field = new DocumentField(fieldName, asList("a", "b"));
        SearchHit hit = new SearchHit(1, null, singletonMap(fieldName, field), null);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Arrays (returned by [" + fieldName + "]) are not supported"));
    }

    public void testExtractSourcePath() {
        FieldHitExtractor fe = getFieldHitExtractor("a.b.c");
        Object value = randomValue();
        DocumentField field = new DocumentField("a.b.c", singletonList(value));
        SearchHit hit = new SearchHit(1, null, null, singletonMap("a.b.c", field), null);
        assertThat(fe.extract(hit), is(value));
    }
    
    public void testMultiValuedSource() {
        FieldHitExtractor fe = getFieldHitExtractor("a");
        Object value = randomValue();
        DocumentField field = new DocumentField("a", asList(value, value));
        SearchHit hit = new SearchHit(1, null, null, singletonMap("a", field), null);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Arrays (returned by [a]) are not supported"));
    }
    
    public void testMultiValuedSourceAllowed() {
        FieldHitExtractor fe = new FieldHitExtractor("a", null, UTC, true);
        Object valueA = randomValue();
        Object valueB = randomValue();
        DocumentField field = new DocumentField("a", asList(valueA, valueB));
        SearchHit hit = new SearchHit(1, null, null, singletonMap("a", field), null);
        assertEquals(valueA, fe.extract(hit));
    }

    public void testGeoShapeExtraction() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, randomBoolean() ? GEO_SHAPE : SHAPE, UTC, false);

        Map<String, Object> map = new HashMap<>(2);
        map.put("coordinates", asList(1d, 2d));
        map.put("type", "Point");
        DocumentField field = new DocumentField(fieldName, singletonList(map));
        SearchHit hit = new SearchHit(1, null, null, singletonMap(fieldName, field), null);

        assertEquals(new GeoShape(1, 2), fe.extract(hit));
    }
    
    public void testMultipleGeoShapeExtraction() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, randomBoolean() ? GEO_SHAPE : SHAPE, UTC, false);
    
        Map<String, Object> map1 = new HashMap<>(2);
        map1.put("coordinates", asList(1d, 2d));
        map1.put("type", "Point");
        Map<String, Object> map2 = new HashMap<>(2);
        map2.put("coordinates", asList(3d, 4d));
        map2.put("type", "Point");
        DocumentField field = new DocumentField(fieldName, asList(map1, map2));
        SearchHit hit = new SearchHit(1, null, singletonMap(fieldName, field), null);

        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Arrays (returned by [" + fieldName + "]) are not supported"));
    
        FieldHitExtractor lenientFe = new FieldHitExtractor(fieldName, randomBoolean() ? GEO_SHAPE : SHAPE, UTC, true);
        assertEquals(new GeoShape(3, 4), lenientFe.extract(new SearchHit(1, null, null, singletonMap(fieldName,
            new DocumentField(fieldName, singletonList(map2))), null)));
    }

    private FieldHitExtractor getFieldHitExtractor(String fieldName) {
        return new FieldHitExtractor(fieldName, null, UTC);
    }

    private Object randomValue() {
        Supplier<Object> value = randomFrom(
            Arrays.asList(
                () -> randomAlphaOfLength(10),
                ESTestCase::randomLong,
                ESTestCase::randomDouble,
                ESTestCase::randomInt,
                () -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
                () -> new BigDecimal("20012312345621343256123456254.20012312345621343256123456254"),
                () -> null
            )
        );
        return value.get();
    }
}
