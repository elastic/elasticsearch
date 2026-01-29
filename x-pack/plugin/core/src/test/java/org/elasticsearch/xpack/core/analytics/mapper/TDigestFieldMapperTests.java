/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TDigestFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return generateRandomFieldValues(100);
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TestAnalyticsPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "tdigest");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> assertTrue(m.ignoreMalformed()));
        checker.registerConflictCheck("digest_type", b -> b.field("digest_type", TDigestExecutionHint.HIGH_ACCURACY));
        checker.registerConflictCheck("compression", b -> b.field("compression", 117));
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    public void testParseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field("centroids", new double[] { 2, 3 }).field("counts", new int[] { 0, 4 }).endObject())
        );
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testParseArrayValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("counts", new int[] { 2, 2, 3 }).field("centroids", new double[] { 2, 2, 3 }).endObject();
                b.startObject().field("counts", new int[] { 2, 2, 3 }).field("centroids", new double[] { 2, 2, 3 }).endObject();
            }
            b.endArray();
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("doesn't support indexing multiple values for the same field in the same document")
        );
    }

    public void testEmptyArrays() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field").field("centroids", new double[] {}).field("counts", new int[] {}).endObject())
        );
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("pre_aggregated")));
        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
    }

    public void testMissingFieldCounts() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("centroids", new double[] { 2, 2 }).endObject()))
        );
        assertThat(e.getCause().getMessage(), containsString("expected field called [counts]"));
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        String randomString = randomAlphaOfLengthBetween(1, 10);
        long randomLong = randomLong();
        double randomDouble = randomDouble();
        boolean randomBoolean = randomBoolean();

        return List.of(
            exampleMalformedValue(b -> b.value(randomString)).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),
            exampleMalformedValue(b -> b.value(randomLong)).errorMatches("Failed to parse object: expecting token of type [START_OBJECT]"),
            exampleMalformedValue(b -> b.value(randomDouble)).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),
            exampleMalformedValue(b -> b.value(randomBoolean)).errorMatches(
                "Failed to parse object: expecting token of type [START_OBJECT]"
            ),
            exampleMalformedValue(b -> b.startObject().endObject()).errorMatches("expected field called [centroids]"),
            exampleMalformedValue(b -> b.startObject().startArray("centroids").value(2).value(2).endArray().endObject()).errorMatches(
                "expected field called [counts]"
            ),
            exampleMalformedValue(b -> b.startObject().startArray("counts").value(2).value(2).endArray().endObject()).errorMatches(
                "expected field called [centroids]"
            ),
            // Make sure that entire sub-object is preserved in synthetic source
            exampleMalformedValue(
                b -> b.startObject()
                    .startArray("centroids")
                    .value(2)
                    .endArray()
                    .field("somefield", randomString)
                    .array("somearray", randomLong, randomLong)
                    .startObject("someobject")
                    .field("nestedfield", randomDouble)
                    .endObject()
                    .endObject()
            ).errorMatches("unknown parameter [somefield]"),
            exampleMalformedValue(b -> b.startArray().value(randomLong).value(randomLong).endArray()).errorMatches(
                "expecting token of type [START_OBJECT] but found [VALUE_NUMBER]"
            )
        );
    }

    public void testIgnoreMalformedSkipsKeyword() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("pre_aggregated").field("type", "tdigest").field("ignore_malformed", true).endObject();
            b.startObject("otherField").field("type", "keyword").endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("pre_aggregated", "value").field("otherField", "value")));
        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("pre_aggregated").field("type", "tdigest").field("ignore_malformed", true).endObject();
            b.startObject("otherField").field("type", "keyword").endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("pre_aggregated", new int[] { 2, 2, 2 }).field("otherField", "value")));
        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("pre_aggregated").field("type", "tdigest").field("ignore_malformed", true).endObject();
            b.startObject("otherField").field("type", "keyword").endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("pre_aggregated").field("centroids", new double[] { 2, 2 }).field("typo", new double[] { 2, 2 }).endObject();
            b.field("otherField", "value");
        }));
        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsObjects() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("pre_aggregated").field("type", "tdigest").field("ignore_malformed", true).endObject();
            b.startObject("otherField").field("type", "keyword").endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("pre_aggregated");
            {
                b.startObject("centroids");
                {
                    b.field("centroids", new double[] { 2, 2 });
                    b.startObject("otherData");
                    {
                        b.startObject("more").field("toto", 1).endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.field("counts", new double[] { 2, 2 });
            }
            b.endObject();
            b.field("otherField", "value");
        }));
        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testIgnoreMalformedSkipsEmpty() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("pre_aggregated").field("type", "tdigest").field("ignore_malformed", true).endObject();
            b.startObject("otherField").field("type", "keyword").endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("pre_aggregated").endObject().field("otherField", "value")));
        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
        assertThat(doc.rootDoc().getField("otherField"), notNullValue());
    }

    public void testMissingFieldValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("counts", new int[] { 2, 2 }).endObject()))
        );
        assertThat(e.getCause().getMessage(), containsString("expected field called [centroids]"));
    }

    public void testUnknownField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field")
                .field("counts", new int[] { 2, 2 })
                .field("centroids", new double[] { 2, 2 })
                .field("unknown", new double[] { 2, 2 })
                .endObject()
        );

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("with unknown parameter [unknown]"));
    }

    public void testFieldArraysDifferentSize() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field").field("counts", new int[] { 2, 2 }).field("centroids", new double[] { 2, 2, 3 }).endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expected same length from [centroids] and [counts] but got [3 != 2]"));
    }

    public void testFieldCountsNotArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field").field("counts", "bah").field("centroids", new double[] { 2, 2, 3 }).endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testFieldCountsStringArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field")
                .field("counts", new String[] { "4", "5", "6" })
                .field("centroids", new double[] { 2, 2, 3 })
                .endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]"));
    }

    public void testFieldValuesStringArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.field("field")
                .startObject()
                .field("counts", new int[] { 4, 5, 6 })
                .field("centroids", new String[] { "2", "2", "3" })
                .endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]"));
    }

    public void testFieldValuesNotArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field").field("counts", new int[] { 2, 2, 3 }).field("centroids", "bah").endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_ARRAY] but found [VALUE_STRING]"));
    }

    public void testCountIsLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field")
                .field("counts", new long[] { 2, 2, Long.MAX_VALUE })
                .field("centroids", new double[] { 2, 2, 3 })
                .endObject()
        );
        ParsedDocument doc = mapper.parse(source);
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testValuesNotInOrder() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.field("field")
                .startObject()
                .field("counts", new int[] { 2, 8, 4 })
                .field("centroids", new double[] { 2, 3, 2 })
                .endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(
            e.getCause().getMessage(),
            containsString(" centroids must be in increasing order, " + "got [2.0] but previous value was [3.0]")
        );
    }

    public void testFieldNotObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(b -> b.field("field", "bah"));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("expecting token of type [START_OBJECT] " + "but found [VALUE_STRING]"));
    }

    public void testNegativeCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field").field("counts", new int[] { 2, 2, -3 }).field("centroids", new double[] { 2, 2, 3 }).endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("[counts] elements must be >= 0 but got -3"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    public void testCannotBeUsedInMultifields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("hist");
            b.field("type", "tdigest");
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [hist] of type [tdigest] can't be used in multifields"));
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }

    public void testArrayValueSyntheticSource() throws Exception {
        DocumentMapper mapper = createSytheticSourceMapperService(
            fieldMapping(b -> b.field("type", "tdigest").field("ignore_malformed", "true"))
        ).documentMapper();

        String randomString = randomAlphaOfLength(10);
        CheckedConsumer<XContentBuilder, IOException> arrayValue = b -> {
            b.startArray("field");
            {
                b.startObject().field("counts", new int[] { 1, 2, 3 }).field("centroids", new double[] { 1, 2, 3 }).endObject();
                b.startObject().field("counts", new int[] { 4, 5, 6 }).field("centroids", new double[] { 4, 5, 6 }).endObject();
                b.value(randomString);
            }
            b.endArray();
        };

        XContentBuilder expected = JsonXContent.contentBuilder().startObject();
        // First value comes from synthetic field loader and so is formatted in a specific format (e.g. centroids always come first).
        // Other values are stored as is as part of ignore_malformed logic for synthetic source.
        {
            expected.startArray("field");
            expected.startObject();
            expected.field("min", 1.0d);
            expected.field("max", 3.0d);
            expected.field("sum", 14.0d);
            expected.field("centroids", new double[] { 1, 2, 3 });
            expected.field("counts", new int[] { 1, 2, 3 });
            expected.endObject();
            expected.startObject().field("counts", new int[] { 4, 5, 6 }).field("centroids", new double[] { 4, 5, 6 }).endObject();
            expected.value(randomString);
            expected.endArray();
        }
        expected.endObject();

        String syntheticSource = syntheticSource(mapper, arrayValue);
        assertEquals(Strings.toString(expected), syntheticSource);
    }

    static Map<String, Object> generateRandomFieldValues(int maxVals) {
        Map<String, Object> value = new LinkedHashMap<>();
        int size = between(0, maxVals);
        TDigestState digest = TDigestState.createWithoutCircuitBreaking(100);
        for (int i = 0; i < size; i++) {
            double sample = randomGaussianDouble();
            int count = randomIntBetween(1, Integer.MAX_VALUE);
            digest.add(sample, count);
        }
        List<Double> centroids = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        double sum = 0.0;
        for (Centroid c : digest.centroids()) {
            centroids.add(c.mean());
            counts.add(c.count());
            sum += c.mean() * c.count();
        }
        double min = digest.getMin();
        double max = digest.getMax();
        if (size > 0) {
            value.put("min", min);
            value.put("max", max);
            value.put("sum", sum);
        }
        value.put("centroids", centroids);
        value.put("counts", counts);

        return value;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new TDigestFieldSyntheticSourceSupport(ignoreMalformed);
    }

    private record TDigestFieldSyntheticSourceSupport(boolean ignoreMalformed) implements SyntheticSourceSupport {
        @Override
        public SyntheticSourceExample example(int maxVals) {
            Map<String, Object> value = generateRandomFieldValues(maxVals);
            return new SyntheticSourceExample(value, value, this::mapping);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "tdigest");
            if (ignoreMalformed) {
                b.field("ignore_malformed", true);
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        // The mapper expects to parse an array of values by default, it's not compatible with array of arrays.
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }

}
