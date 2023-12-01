/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

public class GetStackTracesRequestTests extends ESTestCase {
    public void testSerializationRandomized() throws IOException {
        Integer sampleSize = randomIntBetween(1, Integer.MAX_VALUE);
        Double requestedDuration = randomBoolean() ? randomDoubleBetween(0.001d, Double.MAX_VALUE, true) : null;
        Double awsCostFactor = randomBoolean() ? randomDoubleBetween(0.1d, 5.0d, true) : null;
        Double customCO2PerKWH = randomBoolean() ? randomDoubleBetween(0.000001d, 0.001d, true) : null;
        Double datacenterPUE = randomBoolean() ? randomDoubleBetween(1.0d, 3.0d, true) : null;
        Double perCoreWattX86 = randomBoolean() ? randomDoubleBetween(0.01d, 20.0d, true) : null;
        Double perCoreWattARM64 = randomBoolean() ? randomDoubleBetween(0.01d, 20.0d, true) : null;
        Double customCostPerCoreHour = randomBoolean() ? randomDoubleBetween(0.001d, 1000.0d, true) : null;
        QueryBuilder query = randomBoolean() ? new BoolQueryBuilder() : null;

        GetStackTracesRequest request = new GetStackTracesRequest(
            sampleSize,
            requestedDuration,
            awsCostFactor,
            query,
            null,
            null,
            customCO2PerKWH,
            datacenterPUE,
            perCoreWattX86,
            perCoreWattARM64,
            customCostPerCoreHour
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                GetStackTracesRequest deserialized = new GetStackTracesRequest(in);
                assertEquals(sampleSize, deserialized.getSampleSize());
                assertEquals(awsCostFactor, deserialized.getAwsCostFactor());
                assertEquals(query, deserialized.getQuery());
            }
        }
    }

    private static class GetStackTracesRequestGenerator implements Iterable<GetStackTracesRequest> {
        private final List<Integer> sampleSizes;
        private int sampleSizesPos = 0;

        private final List<Double> requestedDurations;
        private int requestedDurationsPos = 0;
        private final List<Double> awsCostFactors;
        private int awsCostFactorsPos = 0;
        private final List<QueryBuilder> querys;
        private int querysPos = 0;
        private final List<String> indices;
        private int indicesPos = 0;
        private final List<String> stackTraceIds;
        private int stackTraceIdsPos = 0;
        private final List<Double> customCO2PerKWHs;
        private int customCO2PerKWHsPos = 0;
        private final List<Double> datacenterPUEs;
        private int datacenterPUEsPos = 0;
        private final List<Double> perCoreWattX86s;
        private int perCoreWattX86sPos = 0;
        private final List<Double> perCoreWattARM64s;
        private int perCoreWattARM64sPos = 0;
        private final List<Double> customCostPerCoreHours;
        private int customCostPerCoreHoursPos = 0;
        private GetStackTracesRequest curRequest;
        private boolean done = false;

        GetStackTracesRequestGenerator(
            List<Integer> sampleSizes,
            List<Double> requestedDurations,
            List<Double> awsCostFactors,
            List<QueryBuilder> querys,
            List<String> indices,
            List<String> stackTraceIds,
            List<Double> customCO2PerKWHs,
            List<Double> datacenterPUEs,
            List<Double> perCoreWattX86s,
            List<Double> perCoreWattARM64s,
            List<Double> customCostPerCoreHours
        ) {
            this.sampleSizes = sampleSizes;
            this.requestedDurations = requestedDurations;
            this.awsCostFactors = awsCostFactors;
            this.querys = querys;
            this.indices = indices;
            this.stackTraceIds = stackTraceIds;
            this.customCO2PerKWHs = customCO2PerKWHs;
            this.datacenterPUEs = datacenterPUEs;
            this.perCoreWattX86s = perCoreWattX86s;
            this.perCoreWattARM64s = perCoreWattARM64s;
            this.customCostPerCoreHours = customCostPerCoreHours;
        }

        public GetStackTracesRequest createRequest() {
            return new GetStackTracesRequest(
                sampleSizes.get(sampleSizesPos),
                requestedDurations.get(requestedDurationsPos),
                awsCostFactors.get(awsCostFactorsPos),
                querys.get(querysPos),
                indices.get(indicesPos),
                stackTraceIds.get(stackTraceIdsPos),
                customCO2PerKWHs.get(customCO2PerKWHsPos),
                datacenterPUEs.get(datacenterPUEsPos),
                perCoreWattX86s.get(perCoreWattX86sPos),
                perCoreWattARM64s.get(perCoreWattARM64sPos),
                customCostPerCoreHours.get(customCostPerCoreHoursPos)
            );
        }

        public void check(GetStackTracesRequest request) {
            assertEquals(curRequest, request);
        }

        @Override
        public Iterator<GetStackTracesRequest> iterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return done == false;
                }

                @Override
                public GetStackTracesRequest next() {
                    curRequest = createRequest();

                    sampleSizesPos++;
                    if (sampleSizesPos >= sampleSizes.size()) {
                        sampleSizesPos = 0;
                        requestedDurationsPos++;
                        if (requestedDurationsPos >= requestedDurations.size()) {
                            requestedDurationsPos = 0;
                            awsCostFactorsPos++;
                            if (awsCostFactorsPos >= awsCostFactors.size()) {
                                awsCostFactorsPos = 0;
                                querysPos++;
                                if (querysPos >= querys.size()) {
                                    querysPos = 0;
                                    indicesPos++;
                                    if (indicesPos >= indices.size()) {
                                        indicesPos = 0;
                                        stackTraceIdsPos++;
                                        if (stackTraceIdsPos >= stackTraceIds.size()) {
                                            stackTraceIdsPos = 0;
                                            customCO2PerKWHsPos++;
                                            if (customCO2PerKWHsPos >= customCO2PerKWHs.size()) {
                                                customCO2PerKWHsPos = 0;
                                                datacenterPUEsPos++;
                                                if (datacenterPUEsPos >= datacenterPUEs.size()) {
                                                    datacenterPUEsPos = 0;
                                                    perCoreWattX86sPos++;
                                                    if (perCoreWattX86sPos >= perCoreWattX86s.size()) {
                                                        perCoreWattX86sPos = 0;
                                                        perCoreWattARM64sPos++;
                                                        if (perCoreWattARM64sPos >= perCoreWattARM64s.size()) {
                                                            perCoreWattARM64sPos = 0;
                                                            customCostPerCoreHoursPos++;
                                                            if (customCostPerCoreHoursPos >= customCostPerCoreHours.size()) {
                                                                customCostPerCoreHoursPos = 0;
                                                                done = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return curRequest;
                }
            };
        }
    }

    /**
     * Test serialization of all combinations of edge values.
     * Randomized testing doesn't cover these edge cases, or at least the probability is very low.
     */
    public void testSerializationEdgeCases() throws IOException {
        List<Integer> samplesSizes = Arrays.asList(null, 1, Integer.MAX_VALUE);
        List<Double> requestedDurations = Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE);
        List<Double> awsCostFactors = Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE);
        List<QueryBuilder> querys = Arrays.asList(null, new BoolQueryBuilder());
        List<String> indices = Arrays.asList(null, "foo");
        List<String> stackTraceIds = Arrays.asList(null, "foo");
        List<Double> customCO2PerKWHs = Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE);
        List<Double> datacenterPUEs = Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE);
        List<Double> perCoreWattX86s = Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE);
        List<Double> perCoreWattARM64s = Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE);
        List<Double> customCostPerCoreHours = Arrays.asList(null, Double.MIN_VALUE, Double.MAX_VALUE);

        GetStackTracesRequestGenerator generator = new GetStackTracesRequestGenerator(
            samplesSizes,
            requestedDurations,
            awsCostFactors,
            querys,
            indices,
            stackTraceIds,
            customCO2PerKWHs,
            datacenterPUEs,
            perCoreWattX86s,
            perCoreWattARM64s,
            customCostPerCoreHours
        );

        for (GetStackTracesRequest request : generator) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                request.writeTo(out);
                try (
                    NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())
                ) {
                    GetStackTracesRequest deserialized = new GetStackTracesRequest(in);
                    generator.check(deserialized);
                }
            }
        }
    }

    public void testParseValidXContent() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 500)
                .field("requested_duration", 100.54d)
                .startObject("query")
                    .startObject("range")
                        .startObject("@timestamp")
                            .field("gte", "2022-10-05")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        //end::noformat
        )) {

            GetStackTracesRequest request = new GetStackTracesRequest();
            request.parseXContent(content);

            assertEquals(Integer.valueOf(500), request.getSampleSize());
            assertEquals(Double.valueOf(100.54d), request.getRequestedDuration());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());
        }
    }

    public void testParseValidXContentWithCustomIndex() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 2000)
                .field("indices", "my-traces")
                .field("stacktrace_ids", "stacktraces")
                .startObject("query")
                    .startObject("range")
                        .startObject("@timestamp")
                            .field("gte", "2022-10-05")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        //end::noformat
        )) {

            GetStackTracesRequest request = new GetStackTracesRequest();
            request.parseXContent(content);

            assertEquals(Integer.valueOf(2000), request.getSampleSize());
            assertEquals("my-traces", request.getIndices());
            assertEquals("stacktraces", request.getStackTraceIds());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());

            // Expect the default values
            assertEquals(null, request.getRequestedDuration());
        }
    }

    public void testParseXContentUnrecognizedField() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                // should be sample_size
                .field("sample-size", 500)
                .startObject("query")
                    .startObject("range")
                        .startObject("@timestamp")
                            .field("gte", "2022-10-05")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        //end::noformat
        )) {

            GetStackTracesRequest request = new GetStackTracesRequest();
            ParsingException ex = expectThrows(ParsingException.class, () -> request.parseXContent(content));
            assertEquals("Unknown key for a VALUE_NUMBER in [sample-size].", ex.getMessage());
        }
    }

    public void testValidateWrongSampleSize() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            randomIntBetween(Integer.MIN_VALUE, 0),
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertTrue(validationErrors.get(0).contains("[sample_size] must be greater than 0,"));
    }

    public void testValidateStacktraceWithoutIndices() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            1,
            1.0d,
            1.0d,
            null,
            null,
            randomAlphaOfLength(3),
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[stacktrace_ids] must not be set", validationErrors.get(0));
    }

    public void testValidateIndicesWithoutStacktraces() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            null,
            randomAlphaOfLength(5),
            randomFrom("", null),
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[stacktrace_ids] is mandatory", validationErrors.get(0));
    }

    public void testConsidersCustomIndicesInRelatedIndices() {
        String customIndex = randomAlphaOfLength(5);
        GetStackTracesRequest request = new GetStackTracesRequest(
            1,
            1.0d,
            1.0d,
            null,
            customIndex,
            randomAlphaOfLength(3),
            null,
            null,
            null,
            null,
            null
        );
        String[] indices = request.indices();
        assertEquals(4, indices.length);
        assertTrue("custom index not contained in indices list", Set.of(indices).contains(customIndex));
    }

    public void testConsidersDefaultIndicesInRelatedIndices() {
        String customIndex = randomAlphaOfLength(5);
        GetStackTracesRequest request = new GetStackTracesRequest(1, 1.0d, 1.0d, null, null, null, null, null, null, null, null);
        String[] indices = request.indices();
        assertEquals(15, indices.length);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        // to register the query parser
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables());
    }
}
