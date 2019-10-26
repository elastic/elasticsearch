/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.fields;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class ExtractedFieldTests extends ESTestCase {

    public void testValueGivenDocValue() {
        SearchHit hit = new SearchHitBuilder(42).addField("single", "bar").addField("array", Arrays.asList("a", "b")).build();

        ExtractedField single = ExtractedField.newField("single", Collections.singleton("keyword"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(single.value(hit), equalTo(new String[] { "bar" }));

        ExtractedField array = ExtractedField.newField("array", Collections.singleton("keyword"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(array.value(hit), equalTo(new String[] { "a", "b" }));

        ExtractedField missing = ExtractedField.newField("missing",Collections.singleton("keyword"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(missing.value(hit), equalTo(new Object[0]));
    }

    public void testValueGivenScriptField() {
        SearchHit hit = new SearchHitBuilder(42).addField("single", "bar").addField("array", Arrays.asList("a", "b")).build();

        ExtractedField single = ExtractedField.newField("single",Collections.emptySet(),
            ExtractedField.ExtractionMethod.SCRIPT_FIELD);
        assertThat(single.value(hit), equalTo(new String[] { "bar" }));

        ExtractedField array = ExtractedField.newField("array", Collections.emptySet(), ExtractedField.ExtractionMethod.SCRIPT_FIELD);
        assertThat(array.value(hit), equalTo(new String[] { "a", "b" }));

        ExtractedField missing = ExtractedField.newField("missing", Collections.emptySet(), ExtractedField.ExtractionMethod.SCRIPT_FIELD);
        assertThat(missing.value(hit), equalTo(new Object[0]));
    }

    public void testValueGivenSource() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"single\":\"bar\",\"array\":[\"a\",\"b\"]}").build();

        ExtractedField single = ExtractedField.newField("single", Collections.singleton("text"), ExtractedField.ExtractionMethod.SOURCE);
        assertThat(single.value(hit), equalTo(new String[] { "bar" }));

        ExtractedField array = ExtractedField.newField("array", Collections.singleton("text"), ExtractedField.ExtractionMethod.SOURCE);
        assertThat(array.value(hit), equalTo(new String[] { "a", "b" }));

        ExtractedField missing = ExtractedField.newField("missing", Collections.singleton("text"), ExtractedField.ExtractionMethod.SOURCE);
        assertThat(missing.value(hit), equalTo(new Object[0]));
    }

    public void testValueGivenNestedSource() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"level_1\":{\"level_2\":{\"foo\":\"bar\"}}}").build();

        ExtractedField nested = ExtractedField.newField("alias", "level_1.level_2.foo", Collections.singleton("text"),
            ExtractedField.ExtractionMethod.SOURCE);
        assertThat(nested.value(hit), equalTo(new String[] { "bar" }));
    }

    public void testGeoPoint() {
        double lat = 38.897676;
        double lon = -77.03653;
        String[] expected = new String[] {lat + "," + lon};

        // doc_value field
        ExtractedField geo = ExtractedField.newGeoPointField("geo", "geo");
        SearchHit hit = new SearchHitBuilder(42).addField("geo", lat + ", " + lon).build();
        assertThat(geo.value(hit), equalTo(expected));
    }

    public void testGeoShape() {
        double lat = 38.897676;
        double lon = -77.03653;
        String[] expected = new String[] {lat + "," + lon};
        // object format
        SearchHit hit = new SearchHitBuilder(42)
            .setSource("{\"geo\":{\"type\":\"point\", \"coordinates\": [" + lon + ", " + lat + "]}}")
            .build();
        ExtractedField geo = ExtractedField.newGeoShapeField("geo", "geo");
        assertThat(geo.value(hit), equalTo(expected));

        // WKT format
        hit = new SearchHitBuilder(42).setSource("{\"geo\":\"POINT ("+ lon + " " + lat + ")\"}").build();
        geo = ExtractedField.newGeoShapeField("geo", "geo");
        assertThat(geo.value(hit), equalTo(expected));
    }

    public void testValueGivenSourceAndHitWithNoSource() {
        ExtractedField missing = ExtractedField.newField("missing", Collections.singleton("text"), ExtractedField.ExtractionMethod.SOURCE);
        assertThat(missing.value(new SearchHitBuilder(3).build()), equalTo(new Object[0]));
    }

    public void testValueGivenMismatchingMethod() {
        SearchHit hit = new SearchHitBuilder(42).addField("a", 1).setSource("{\"b\":2}").build();

        ExtractedField invalidA = ExtractedField.newField("a", Collections.singleton("text"), ExtractedField.ExtractionMethod.SOURCE);
        assertThat(invalidA.value(hit), equalTo(new Object[0]));
        ExtractedField validA = ExtractedField.newField("a", Collections.singleton("keyword"), ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(validA.value(hit), equalTo(new Integer[] { 1 }));

        ExtractedField invalidB = ExtractedField.newField("b", Collections.singleton("keyword"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(invalidB.value(hit), equalTo(new Object[0]));
        ExtractedField validB = ExtractedField.newField("b", Collections.singleton("text"), ExtractedField.ExtractionMethod.SOURCE);
        assertThat(validB.value(hit), equalTo(new Integer[] { 2 }));
    }

    public void testValueGivenEmptyHit() {
        SearchHit hit = new SearchHitBuilder(42).build();

        ExtractedField docValue = ExtractedField.newField("a", Collections.singleton("text"), ExtractedField.ExtractionMethod.SOURCE);
        assertThat(docValue.value(hit), equalTo(new Object[0]));

        ExtractedField sourceField = ExtractedField.newField("b", Collections.singleton("keyword"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(sourceField.value(hit), equalTo(new Object[0]));
    }

    public void testNewTimeFieldGivenSource() {
        expectThrows(IllegalArgumentException.class, () -> ExtractedField.newTimeField("time", Collections.singleton("date"),
            ExtractedField.ExtractionMethod.SOURCE));
    }

    public void testValueGivenStringTimeField() {
        final long millis = randomLong();
        final SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", Long.toString(millis)).build();
        final ExtractedField timeField = ExtractedField.newTimeField("time", Collections.singleton("date"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
    }

    public void testValueGivenLongTimeField() {
        final long millis = randomLong();
        final SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", millis).build();
        final ExtractedField timeField = ExtractedField.newTimeField("time", Collections.singleton("date"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
    }

    public void testValueGivenPre6xTimeField() {
        // Prior to 6.x, timestamps were simply `long` milliseconds-past-the-epoch values
        final long millis = randomLong();
        final SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", millis).build();
        final ExtractedField timeField = ExtractedField.newTimeField("time", Collections.singleton("date"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
    }

    public void testValueGivenUnknownFormatTimeField() {
        final SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", new Object()).build();
        final ExtractedField timeField = ExtractedField.newTimeField("time", Collections.singleton("date"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(expectThrows(IllegalStateException.class, () -> timeField.value(hit)).getMessage(),
            startsWith("Unexpected value for a time field"));
    }

    public void testAliasVersusName() {
        SearchHit hit = new SearchHitBuilder(42).addField("a", 1).addField("b", 2).build();

        ExtractedField field = ExtractedField.newField("a", "a", Collections.singleton("int"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(field.getAlias(), equalTo("a"));
        assertThat(field.getName(), equalTo("a"));
        assertThat(field.value(hit), equalTo(new Integer[] { 1 }));

        hit = new SearchHitBuilder(42).addField("a", 1).addField("b", 2).build();

        field = ExtractedField.newField("a", "b", Collections.singleton("int"), ExtractedField.ExtractionMethod.DOC_VALUE);
        assertThat(field.getAlias(), equalTo("a"));
        assertThat(field.getName(), equalTo("b"));
        assertThat(field.value(hit), equalTo(new Integer[] { 2 }));
    }

    public void testGetDocValueFormat() {
        for (ExtractedField.ExtractionMethod method : ExtractedField.ExtractionMethod.values()) {
            assertThat(ExtractedField.newField("f", Collections.emptySet(), method).getDocValueFormat(), equalTo(null));
        }
        assertThat(ExtractedField.newTimeField("doc_value_time", Collections.singleton("date"),
            ExtractedField.ExtractionMethod.DOC_VALUE).getDocValueFormat(), equalTo("epoch_millis"));
        assertThat(ExtractedField.newTimeField("source_time", Collections.emptySet(),
            ExtractedField.ExtractionMethod.SCRIPT_FIELD).getDocValueFormat(), equalTo("epoch_millis"));
    }
}
