/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TimeSeriesMetadataEncodingTests extends ESTestCase {
    /** Equal label sets get equal bytes whatever order their labels come in, so a label container keys groups as it is. */
    public void testEqualLabelSetsGetEqualBytes() {
        Map<String, List<String>> labels = new TreeMap<>();
        for (int i = 0; i < between(1, 20); i++) {
            labels.put(randomAlphaOfLengthBetween(1, 10), randomList(1, 3, () -> randomRealisticUnicodeOfLengthBetween(1, 30)));
        }
        List<String> names = new ArrayList<>(labels.keySet());
        Collections.shuffle(names, random());
        Map<String, List<String>> shuffled = new LinkedHashMap<>();
        names.forEach(name -> shuffled.put(name, labels.get(name)));
        assertThat(TimeSeriesMetadataEncoding.encode(shuffled), equalTo(TimeSeriesMetadataEncoding.encode(labels)));
    }

    /**
     * A label is found by name among any number of labels, whatever width the table of where they start takes, and a name that is
     * not there is not found.
     */
    public void testFindsALabelByName() {
        // a few short labels take a table of one or two bytes per label, many long ones of two or four
        for (int valueLength : List.of(1, 50, 1000)) {
            Map<String, List<String>> labels = new TreeMap<>();
            int count = valueLength == 1 ? between(1, 20) : between(70, 200);
            for (int i = 0; i < count; i++) {
                labels.put("l" + randomAlphaOfLengthBetween(1, 8), randomList(1, 3, () -> randomAlphaOfLength(valueLength)));
            }
            TimeSeriesMetadataEncoding.Reader reader = new TimeSeriesMetadataEncoding.Reader().reset(
                TimeSeriesMetadataEncoding.encode(labels)
            );
            assertThat(reader.count(), equalTo(labels.size()));
            int position = 0;
            for (var label : labels.entrySet()) {
                int found = reader.find(new BytesRef(label.getKey()));
                assertThat(found, equalTo(position++));
                assertThat(reader.name(found, new BytesRef()).utf8ToString(), equalTo(label.getKey()));
                assertThat(
                    reader.values(found, new ArrayList<>()).stream().map(BytesRef::utf8ToString).toList(),
                    equalTo(label.getValue())
                );
            }
            assertThat(reader.find(new BytesRef("a")), equalTo(-1));
            assertThat(reader.find(new BytesRef("m")), equalTo(-labels.size() - 1));
        }
    }

    /** The values of a label keep their order: another order is another label set. */
    public void testValuesKeepTheirOrder() {
        assertThat(
            TimeSeriesMetadataEncoding.encode(Map.of("pod", List.of("a", "b"))),
            not(equalTo(TimeSeriesMetadataEncoding.encode(Map.of("pod", List.of("b", "a")))))
        );
    }

    public void testRendersAsTheJsonObjectOfItsLabels() {
        BytesRef encoded = TimeSeriesMetadataEncoding.encode(
            Map.of("pod", List.of("a"), "job", List.of("api"), "node", List.of("n\"1", "λ"), "empty", List.of())
        );
        assertThat(
            TimeSeriesMetadataEncoding.toJson(encoded).utf8ToString(),
            equalTo("{\"job\":\"api\",\"node\":[\"n\\\"1\",\"λ\"],\"pod\":\"a\"}")
        );
        assertThat(TimeSeriesMetadataEncoding.toJson(TimeSeriesMetadataEncoding.EMPTY).utf8ToString(), equalTo("{}"));
    }

    /** A source document reads as the label container of its dimensions: nested, prefixed and in any order. */
    public void testReadsASourceDocument() {
        BytesRef encoded = TimeSeriesMetadataEncoding.fromSource(
            new BytesRef("{\"pod\":\"a\",\"labels\":{\"job\":\"api\"},\"k8s\":{\"node\":[\"n\",\"\",\"m\"],\"empty\":\"\"}}")
        );
        assertThat(
            encoded,
            equalTo(TimeSeriesMetadataEncoding.encode(Map.of("job", List.of("api"), "k8s.node", List.of("n", "m"), "pod", List.of("a"))))
        );
    }

    /** Bytes that are not a label container fail rather than reading as some other label set. */
    public void testMalformedBytesFail() {
        BytesRef valid = TimeSeriesMetadataEncoding.encode(Map.of("pod", List.of("a")));
        for (BytesRef malformed : List.of(
            new BytesRef("not a label container"),
            new BytesRef(new byte[] { 0 }),
            new BytesRef(valid.bytes, valid.offset, valid.length - 1),
            new BytesRef(new byte[] { 3, 'p', 'o', 'd', 0 }),
            new BytesRef(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 1 })
        )) {
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadataEncoding.toJson(malformed));
        }
    }
}
