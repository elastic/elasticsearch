/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import io.github.nik9000.mapmatcher.MapMatcher;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.TimeSeriesIdGenerator.ObjectComponent;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class TimeSeriesIdGeneratorTests extends ESTestCase {
    /**
     * Test with non-randomized string for sanity checking.
     */
    public void testStrings() throws IOException {
        Map<String, Object> doc = Map.of("a", "foo", "b", "bar", "c", "baz", "o", Map.of("e", "bort"));
        assertMap(
            TimeSeriesIdGenerator.parse(keywordTimeSeriesIdGenerator().generate(parser(doc)).streamInput()),
            matchesMap().entry("a", "foo").entry("o.e", "bort")
        );
    }

    public void testKeywordTooLong() throws IOException {
        Map<String, Object> doc = Map.of("a", "more_than_1024_bytes".repeat(52));
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> keywordTimeSeriesIdGenerator().generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: longer than [1024] bytes [1040]"));
    }

    public void testKeywordTooLongUtf8() throws IOException {
        String theWordLong = "長い";
        Map<String, Object> doc = Map.of("a", theWordLong.repeat(200));
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> keywordTimeSeriesIdGenerator().generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: longer than [1024] bytes [1200]"));
    }

    public void testKeywordNull() throws IOException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("a", null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> keywordTimeSeriesIdGenerator().generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: null values not allowed"));
    }

    private TimeSeriesIdGenerator keywordTimeSeriesIdGenerator() {
        return TimeSeriesIdGenerator.build(
            new ObjectComponent(Map.of("a", keywordComponent(), "o", new ObjectComponent(Map.of("e", keywordComponent()))))
        );
    }

    /**
     * Test with non-randomized longs for sanity checking.
     */
    public void testLong() throws IOException {
        Map<String, Object> doc = Map.of("a", 1, "b", -1, "c", "baz", "o", Map.of("e", "1234"));
        assertMap(
            TimeSeriesIdGenerator.parse(timeSeriedIdForNumberType(NumberType.LONG).generate(parser(doc)).streamInput()),
            matchesMap().entry("a", 1L).entry("o.e", 1234L)
        );
    }

    public void testLongInvalidString() throws IOException {
        Map<String, Object> doc = Map.of("a", "not_a_long");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.LONG).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: For input string: \"not_a_long\""));
    }

    public void testLongNull() throws IOException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("a", null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.LONG).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), startsWith("error extracting dimension [a]: null values not allowed"));
    }

    /**
     * Test with non-randomized integers for sanity checking.
     */
    public void testInteger() throws IOException {
        Map<String, Object> doc = Map.of("a", 1, "b", -1, "c", "baz", "o", Map.of("e", Integer.MIN_VALUE));
        assertMap(
            TimeSeriesIdGenerator.parse(timeSeriedIdForNumberType(NumberType.INTEGER).generate(parser(doc)).streamInput()),
            matchesMap().entry("a", 1L).entry("o.e", (long) Integer.MIN_VALUE)
        );
    }

    public void testIntegerInvalidString() throws IOException {
        Map<String, Object> doc = Map.of("a", "not_an_int");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.INTEGER).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: For input string: \"not_an_int\""));
    }

    public void testIntegerOutOfRange() throws IOException {
        Map<String, Object> doc = Map.of("a", Long.MAX_VALUE);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.INTEGER).generate(parser(doc)).streamInput()
        );
        assertThat(
            e.getMessage(),
            startsWith("error extracting dimension [a]: Numeric value (" + Long.MAX_VALUE + ") out of range of int")
        );
    }

    public void testIntegerNull() throws IOException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("a", null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.INTEGER).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), startsWith("error extracting dimension [a]: null values not allowed"));
    }

    /**
     * Test with non-randomized shorts for sanity checking.
     */
    public void testShort() throws IOException {
        Map<String, Object> doc = Map.of("a", 1, "b", -1, "c", "baz", "o", Map.of("e", (int) Short.MIN_VALUE));
        assertMap(
            TimeSeriesIdGenerator.parse(timeSeriedIdForNumberType(NumberType.SHORT).generate(parser(doc)).streamInput()),
            matchesMap().entry("a", 1L).entry("o.e", (long) Short.MIN_VALUE)
        );
    }

    public void testShortInvalidString() throws IOException {
        Map<String, Object> doc = Map.of("a", "not_a_short");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.SHORT).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: For input string: \"not_a_short\""));
    }

    public void testShortOutOfRange() throws IOException {
        Map<String, Object> doc = Map.of("a", Long.MAX_VALUE);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.SHORT).generate(parser(doc)).streamInput()
        );
        assertThat(
            e.getMessage(),
            startsWith("error extracting dimension [a]: Numeric value (" + Long.MAX_VALUE + ") out of range of int")
        );
    }

    public void testShortNull() throws IOException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("a", null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.SHORT).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), startsWith("error extracting dimension [a]: null values not allowed"));
    }

    /**
     * Test with non-randomized shorts for sanity checking.
     */
    public void testByte() throws IOException {
        Map<String, Object> doc = Map.of("a", 1, "b", -1, "c", "baz", "o", Map.of("e", (int) Byte.MIN_VALUE));
        assertMap(
            TimeSeriesIdGenerator.parse(timeSeriedIdForNumberType(NumberType.BYTE).generate(parser(doc)).streamInput()),
            matchesMap().entry("a", 1L).entry("o.e", (long) Byte.MIN_VALUE)
        );
    }

    public void testByteInvalidString() throws IOException {
        Map<String, Object> doc = Map.of("a", "not_a_byte");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.BYTE).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: For input string: \"not_a_byte\""));
    }

    public void testByteOutOfRange() throws IOException {
        Map<String, Object> doc = Map.of("a", Long.MAX_VALUE);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.BYTE).generate(parser(doc)).streamInput()
        );
        assertThat(
            e.getMessage(),
            startsWith("error extracting dimension [a]: Numeric value (" + Long.MAX_VALUE + ") out of range of int")
        );
    }

    public void testByteNull() throws IOException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("a", null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> timeSeriedIdForNumberType(NumberType.BYTE).generate(parser(doc)).streamInput()
        );
        assertThat(e.getMessage(), startsWith("error extracting dimension [a]: null values not allowed"));
    }

    private TimeSeriesIdGenerator timeSeriedIdForNumberType(NumberType numberType) {
        return TimeSeriesIdGenerator.build(
            new ObjectComponent(
                Map.of(
                    "a",
                    numberType.timeSeriesIdGenerator(null, true),
                    "o",
                    new ObjectComponent(Map.of("e", numberType.timeSeriesIdGenerator(null, true)))
                )
            )
        );
    }

    /**
     * Test with non-randomized ips for sanity checking.
     */
    public void testIp() throws IOException {
        Map<String, Object> doc = Map.of("a", "192.168.0.1", "b", -1, "c", "baz", "o", Map.of("e", "255.255.255.1"));
        assertMap(
            TimeSeriesIdGenerator.parse(timeSeriedIdForIp().generate(parser(doc)).streamInput()),
            matchesMap().entry("a", "192.168.0.1").entry("o.e", "255.255.255.1")
        );
    }

    public void testIpInvalidString() throws IOException {
        Map<String, Object> doc = Map.of("a", "not_an_ip");
        Exception e = expectThrows(IllegalArgumentException.class, () -> timeSeriedIdForIp().generate(parser(doc)).streamInput());
        assertThat(e.getMessage(), equalTo("error extracting dimension [a]: 'not_an_ip' is not an IP string literal."));
    }

    public void testIpNull() throws IOException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("a", null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> timeSeriedIdForIp().generate(parser(doc)).streamInput());
        assertThat(e.getMessage(), startsWith("error extracting dimension [a]: null values not allowed"));
    }

    private TimeSeriesIdGenerator timeSeriedIdForIp() {
        return TimeSeriesIdGenerator.build(
            new ObjectComponent(
                Map.of(
                    "a",
                    IpFieldMapper.timeSeriesIdGenerator(null),
                    "o",
                    new ObjectComponent(Map.of("e", IpFieldMapper.timeSeriesIdGenerator(null)))
                )
            )
        );
    }

    /**
     * Tests when the total of the tsid is more than 32k.
     */
    public void testVeryLarge() {
        String large = "many words ".repeat(50);
        Map<String, Object> doc = new HashMap<>();
        Map<String, TimeSeriesIdGenerator.Component> components = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            doc.put("d" + i, large);
            components.put("d" + i, keywordComponent());
        }
        TimeSeriesIdGenerator gen = TimeSeriesIdGenerator.build(new ObjectComponent(components));
        Exception e = expectThrows(IllegalArgumentException.class, () -> gen.generate(parser(doc)));
        assertThat(e.getMessage(), equalTo("tsid longer than [32766] bytes [55691]"));
    }

    /**
     * Sending the same document twice produces the same value.
     */
    public void testSameGenConsistentForSameDoc() throws IOException {
        Map<String, Object> doc = randomDoc(between(1, 100), between(0, 2));
        TimeSeriesIdGenerator gen = TimeSeriesIdGenerator.build(objectComponentForDimensions(randomDimensionsFromDoc(doc)));
        assertThat(gen.generate(parser(doc)), equalTo(gen.generate(parser(doc))));
    }

    /**
     * Non dimension fields don't influence the value of the dimension.
     */
    public void testExtraFieldsDoNotMatter() throws IOException {
        Map<String, Object> doc = randomDoc(between(1, 100), between(0, 2));
        Map<String, Object> dimensions = randomDimensionsFromDoc(doc);
        TimeSeriesIdGenerator gen = TimeSeriesIdGenerator.build(objectComponentForDimensions(dimensions));
        assertThat(gen.generate(parser(dimensions)), equalTo(gen.generate(parser(doc))));
    }

    /**
     * The order that the dimensions appear in the document do not influence the value.
     */
    public void testOrderDoesNotMatter() throws IOException {
        Map<String, Object> doc = randomDoc(between(1, 100), between(0, 2));
        Map<String, Object> dimensions = randomDimensionsFromDoc(doc);
        TimeSeriesIdGenerator gen = TimeSeriesIdGenerator.build(objectComponentForDimensions(dimensions));
        assertThat(gen.generate(parser(shuffled(doc))), equalTo(gen.generate(parser(doc))));
    }

    /**
     * Dimensions that appear in the generator but not in the document don't influence the value.
     */
    public void testUnusedExtraDimensions() throws IOException {
        Map<String, Object> doc = randomDoc(between(1, 100), between(0, 2));
        Map<String, Object> dimensions = randomDimensionsFromDoc(doc);
        TimeSeriesIdGenerator small = TimeSeriesIdGenerator.build(objectComponentForDimensions(dimensions));
        dimensions.put(randomValueOtherThanMany(doc::containsKey, () -> randomAlphaOfLength(5)), randomAlphaOfLength(3));
        TimeSeriesIdGenerator large = TimeSeriesIdGenerator.build(objectComponentForDimensions(dimensions));

        assertThat(large.generate(parser(doc)), equalTo(small.generate(parser(doc))));
    }

    /**
     * Different values for dimensions change the result.
     */
    public void testDifferentValues() throws IOException {
        Map<String, Object> orig = randomDoc(between(1, 100), between(0, 2));
        Map<String, Object> dimensions = randomDimensionsFromDoc(orig);
        Map<String, Object> modified = modifyDimensionValue(orig, dimensions);
        TimeSeriesIdGenerator gen = TimeSeriesIdGenerator.build(objectComponentForDimensions(dimensions));
        assertThat(gen.generate(parser(modified)), not(equalTo(gen.generate(parser(orig)))));
    }

    public void testParse() throws IOException {
        Map<String, Object> doc = randomDoc(between(1, 100), between(0, 2));
        Map<String, Object> dimensions = randomDimensionsFromDoc(doc);
        TimeSeriesIdGenerator gen = TimeSeriesIdGenerator.build(objectComponentForDimensions(dimensions));
        assertMap(TimeSeriesIdGenerator.parse(gen.generate(parser(doc)).streamInput()), expectedParsedDimensions(dimensions));
        assertMap(TimeSeriesIdGenerator.parse(gen.generate(parser(shuffled(doc))).streamInput()), expectedParsedDimensions(dimensions));
    }

    private MapMatcher expectedParsedDimensions(Map<String, Object> originalDimensions) {
        return flatten(matchesMap(), null, originalDimensions);
    }

    private MapMatcher flatten(MapMatcher result, String name, Map<?, ?> originalDimensions) {
        for (Map.Entry<?, ?> d : originalDimensions.entrySet()) {
            String nextName = name == null ? d.getKey().toString() : name + "." + d.getKey();
            if (d.getValue() instanceof Map) {
                result = flatten(result, nextName, (Map<?, ?>) d.getValue());
            } else {
                result = result.entry(nextName, d.getValue());
            }
        }
        return result;
    }

    private Map<String, Object> modifyDimensionValue(Map<?, ?> doc, Map<?, ?> dimensions) {
        Object keyToModify = randomFrom(dimensions.keySet());

        Map<String, Object> result = new LinkedHashMap<>(doc.size());
        for (Map.Entry<?, ?> e : doc.entrySet()) {
            if (e.getKey().equals(keyToModify)) {
                Object val = e.getValue();
                Object modified = val instanceof Map
                    ? modifyDimensionValue((Map<?, ?>) val, (Map<?, ?>) dimensions.get(e.getKey()))
                    : val + "modified";
                result.put(e.getKey().toString(), modified);
            } else {
                result.put(e.getKey().toString(), e.getValue());
            }
        }
        return result;
    }

    /**
     * Two documents with the same *values* but different dimension keys will generate
     * different {@code _tsid}s.
     */
    public void testDifferentDimensions() throws IOException {
        Map<String, Object> origDoc = randomDoc(between(1, 10), between(0, 2));
        Map<String, Object> origDimensions = randomDimensionsFromDoc(origDoc);
        TimeSeriesIdGenerator origGen = TimeSeriesIdGenerator.build(objectComponentForDimensions(origDimensions));
        Tuple<Map<String, Object>, Map<String, Object>> modified = modifyDimensionName(origDoc, origDimensions);
        TimeSeriesIdGenerator modGen = TimeSeriesIdGenerator.build(objectComponentForDimensions(modified.v2()));
        assertThat(modGen.generate(parser(modified.v1())), not(equalTo(origGen.generate(parser(origDoc)))));
    }

    private Tuple<Map<String, Object>, Map<String, Object>> modifyDimensionName(Map<?, ?> doc, Map<?, ?> dimensions) {
        Object keyToModify = randomFrom(dimensions.keySet());

        Map<String, Object> modifiedDoc = new LinkedHashMap<>(doc.size());
        Map<String, Object> modifiedDimensions = new LinkedHashMap<>(doc.size());
        for (Map.Entry<?, ?> e : doc.entrySet()) {
            if (e.getKey().equals(keyToModify)) {
                if (e.getValue() instanceof Map) {
                    Tuple<Map<String, Object>, Map<String, Object>> modifiedSub = modifyDimensionName(
                        (Map<?, ?>) e.getValue(),
                        (Map<?, ?>) dimensions.get(e.getKey())
                    );
                    modifiedDoc.put(e.getKey().toString(), modifiedSub.v1());
                    modifiedDimensions.put(e.getKey().toString(), modifiedSub.v2());
                } else {
                    String modifiedKey = e.getKey() + "modified";
                    modifiedDoc.put(modifiedKey, e.getValue());
                    modifiedDimensions.put(modifiedKey, e.getValue());
                }
            } else {
                modifiedDoc.put(e.getKey().toString(), e.getValue());
                if (dimensions.containsKey(e.getKey())) {
                    modifiedDimensions.put(e.getKey().toString(), e.getValue());
                }
            }
        }
        return new Tuple<>(modifiedDoc, modifiedDimensions);
    }

    /**
     * Documents with fewer dimensions have a different value.
     */
    public void testFewerDimensions() throws IOException {
        Map<String, Object> orig = randomDoc(between(2, 100), between(0, 2));
        Map<String, Object> dimensions = randomDimensionsFromDoc(orig, 2, 10);
        Map<String, Object> modified = removeDimension(orig, dimensions);
        TimeSeriesIdGenerator gen = TimeSeriesIdGenerator.build(objectComponentForDimensions(dimensions));
        assertThat(gen.generate(parser(modified)), not(equalTo(gen.generate(parser(orig)))));
    }

    public void testEmpty() throws IOException {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> TimeSeriesIdGenerator.build(null).generate(parser(Map.of())).streamInput()
        );
        assertThat(e.getMessage(), equalTo("There aren't any mapped dimensions"));
    }

    /**
     * Removes one of the dimensions from a document.
     */
    private Map<String, Object> removeDimension(Map<?, ?> doc, Map<?, ?> dimensions) {
        Object keyToRemove = randomFrom(dimensions.keySet());

        Map<String, Object> result = new LinkedHashMap<>(doc.size());
        for (Map.Entry<?, ?> e : doc.entrySet()) {
            if (e.getKey().equals(keyToRemove)) {
                // If the dimension is an object then randomly remove it entirely or one of its leaf values
                if (e.getValue() instanceof Map && randomBoolean()) {
                    result.put(e.getKey().toString(), removeDimension((Map<?, ?>) e.getValue(), (Map<?, ?>) dimensions.get(e.getKey())));
                }
            } else {
                result.put(e.getKey().toString(), e.getValue());
            }
        }
        return result;
    }

    private LinkedHashMap<String, Object> randomDoc(int count, int subDepth) {
        int keyLength = (int) Math.log(count) + 1;
        LinkedHashMap<String, Object> doc = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            String key = randomValueOtherThanMany(doc::containsKey, () -> randomAlphaOfLength(keyLength));
            Object sub = subDepth <= 0 || randomBoolean() ? randomAlphaOfLength(5) : randomDoc(count, subDepth - 1);
            doc.put(key, sub);
        }
        return doc;
    }

    /**
     * Extract a random subset of a document to use as dimensions.
     */
    private LinkedHashMap<String, Object> randomDimensionsFromDoc(Map<?, ?> doc) {
        return randomDimensionsFromDoc(doc, 1, 10);
    }

    /**
     * Extract a random subset of a document to use as dimensions.
     */
    private LinkedHashMap<String, Object> randomDimensionsFromDoc(Map<?, ?> doc, int min, int max) {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        int dimensionCount = between(min, Math.min(doc.size(), max));
        for (Map.Entry<?, ?> dim : randomSubsetOf(dimensionCount, doc.entrySet())) {
            Object sub = dim.getValue() instanceof Map ? randomDimensionsFromDoc((Map<?, ?>) dim.getValue()) : dim.getValue();
            result.put(dim.getKey().toString(), sub);
        }
        return result;
    }

    private TimeSeriesIdGenerator.ObjectComponent objectComponentForDimensions(Map<?, ?> docDimensions) {
        Map<String, TimeSeriesIdGenerator.Component> subs = new HashMap<>(docDimensions.size());
        for (Map.Entry<?, ?> dim : docDimensions.entrySet()) {
            subs.put(dim.getKey().toString(), componentForRepresentativeValue(dim.getValue()));
        }
        return new TimeSeriesIdGenerator.ObjectComponent(subs);
    }

    private TimeSeriesIdGenerator.Component componentForRepresentativeValue(Object value) {
        if (value instanceof Map) {
            return objectComponentForDimensions((Map<?, ?>) value);
        }
        if (value instanceof String) {
            try {
                InetAddresses.forString((String) value);
                return IpFieldMapper.timeSeriesIdGenerator(null);
            } catch (IllegalArgumentException e) {
                return keywordComponent();
            }
        }
        if (value instanceof Number) {
            return NumberFieldMapper.NumberType.LONG.timeSeriesIdGenerator(null, false);
        }
        throw new IllegalArgumentException("Unknown dimension type [" + value + "][" + value.getClass() + "]");
    }

    private TimeSeriesIdGenerator.Component keywordComponent() {
        return KeywordFieldMapper.timeSeriesIdGenerator(null);
    }

    private XContentParser parser(Map<String, Object> doc) throws IOException {
        // Convert the map to json so the parsers don't choke on the methods MapXContentParser doesn't implement
        return createParser(
            JsonXContent.contentBuilder()
                .copyCurrentStructure(
                    new MapXContentParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.IGNORE_DEPRECATIONS,
                        doc,
                        randomFrom(XContentType.values())
                    )
                )
        );
    }

    private LinkedHashMap<String, Object> shuffled(Map<?, ?> orig) {
        List<Map.Entry<?, ?>> entries = new ArrayList<>(orig.entrySet());
        Collections.shuffle(entries, random());
        LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>(orig.size());
        for (Map.Entry<?, ?> e : entries) {
            Object sub = e.getValue() instanceof Map ? shuffled((Map<?, ?>) e.getValue()) : e.getValue();
            result.put(e.getKey().toString(), sub);
        }
        return result;
    }
}
