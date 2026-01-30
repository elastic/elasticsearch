/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

public class GetStackTracesRequestTests extends ESTestCase {
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

            assertEquals(500, request.getSampleSize());
            assertEquals(Double.valueOf(100.54d), request.getRequestedDuration());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());
            // Expect the default values
            assertNull(request.getIndices());
            assertNull(request.getStackTraceIdsField());
            assertNull(request.getAggregationFields());
            assertNull(request.getAwsCostFactor());
            assertNull(request.getAzureCostFactor());
            assertNull(request.getCustomCO2PerKWH());
            assertNull(request.getCustomDatacenterPUE());
            assertNull(request.getCustomCostPerCoreHour());
            assertNull(request.getCustomPerCoreWattX86());
            assertNull(request.getCustomPerCoreWattARM64());
        }
    }

    public void testParseValidXContentWithCustomIndex() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 2000)
                .field("indices", new String[] {"my-traces"})
                .field("stacktrace_ids_field", "stacktraces")
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

            assertEquals(2000, request.getSampleSize());
            assertArrayEquals(new String[] { "my-traces" }, request.getIndices());
            assertEquals("stacktraces", request.getStackTraceIdsField());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());

            // Expect the default values
            assertNull(request.getRequestedDuration());
            assertNull(request.getAggregationFields());
            assertNull(request.getAwsCostFactor());
            assertNull(request.getAzureCostFactor());
            assertNull(request.getCustomCO2PerKWH());
            assertNull(request.getCustomDatacenterPUE());
            assertNull(request.getCustomCostPerCoreHour());
            assertNull(request.getCustomPerCoreWattX86());
            assertNull(request.getCustomPerCoreWattARM64());
        }
    }

    public void testParseValidXContentWithOneAggregationField() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 2000)
                .field("indices", new String[] {"my-traces"})
                .field("stacktrace_ids_field", "stacktraces")
                .field("aggregation_fields", new String[] {"service"})
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

            assertEquals(2000, request.getSampleSize());
            assertArrayEquals(new String[] { "my-traces" }, request.getIndices());
            assertEquals("stacktraces", request.getStackTraceIdsField());
            assertArrayEquals(new String[] { "service" }, request.getAggregationFields());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());

            // Expect the default values
            assertNull(request.getRequestedDuration());
            assertNull(request.getAwsCostFactor());
            assertNull(request.getAzureCostFactor());
            assertNull(request.getCustomCO2PerKWH());
            assertNull(request.getCustomDatacenterPUE());
            assertNull(request.getCustomCostPerCoreHour());
            assertNull(request.getCustomPerCoreWattX86());
            assertNull(request.getCustomPerCoreWattARM64());
        }
    }

    public void testParseValidXContentWithMultipleAggregationFields() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 2000)
                .field("indices", new String[] {"my-traces"})
                .field("stacktrace_ids_field", "stacktraces")
                .field("aggregation_fields", new String[] {"service", "transaction"})
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

            assertEquals(2000, request.getSampleSize());
            assertArrayEquals(new String[] { "my-traces" }, request.getIndices());
            assertEquals("stacktraces", request.getStackTraceIdsField());
            assertArrayEquals(new String[] { "service", "transaction" }, request.getAggregationFields());
            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());

            // Expect the default values
            assertNull(request.getRequestedDuration());
            assertNull(request.getAwsCostFactor());
            assertNull(request.getAzureCostFactor());
            assertNull(request.getCustomCO2PerKWH());
            assertNull(request.getCustomDatacenterPUE());
            assertNull(request.getCustomCostPerCoreHour());
            assertNull(request.getCustomPerCoreWattX86());
            assertNull(request.getCustomPerCoreWattARM64());
        }
    }

    public void testParseValidXContentWithCustomCostAndCO2Data() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
            .startObject()
                .field("sample_size", 2000)
                .field("requested_duration", 100.54d)
                .field("aws_cost_factor", 7.3d)
                .field("azure_cost_factor", 6.4d)
                .field("co2_per_kwh", 22.4d)
                .field("datacenter_pue", 1.05d)
                .field("cost_per_core_hour", 3.32d)
                .field("per_core_watt_x86", 7.2d)
                .field("per_core_watt_arm64", 2.82d)
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

            assertEquals(2000, request.getSampleSize());
            assertEquals(Double.valueOf(100.54d), request.getRequestedDuration());
            assertEquals(Double.valueOf(7.3d), request.getAwsCostFactor());
            assertEquals(Double.valueOf(6.4d), request.getAzureCostFactor());
            assertEquals(Double.valueOf(22.4d), request.getCustomCO2PerKWH());
            assertEquals(Double.valueOf(1.05d), request.getCustomDatacenterPUE());
            assertEquals(Double.valueOf(3.32d), request.getCustomCostPerCoreHour());
            assertEquals(Double.valueOf(7.2d), request.getCustomPerCoreWattX86());
            assertEquals(Double.valueOf(2.82d), request.getCustomPerCoreWattARM64());

            // a basic check suffices here
            assertEquals("@timestamp", ((RangeQueryBuilder) request.getQuery()).fieldName());

            // Expect the default values
            assertNull(request.getIndices());
            assertNull(request.getStackTraceIdsField());
            assertNull(request.getAggregationFields());
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

    public void testParseXContentCustomIndexNoArray() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
                .startObject()
                    .field("sample_size", 2000)
                    .field("indices", "my-traces")
                    .field("stacktrace_ids_field", "stacktraces")
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
            assertEquals("Unknown key for a VALUE_STRING in [indices].", ex.getMessage());
        }
    }

    public void testParseXContentCustomIndexNullValues() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
                .startObject()
                    .field("sample_size", 2000)
                    .field("indices", new String[] {"my-traces", null})
                    .field("stacktrace_ids_field", "stacktraces")
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
            assertEquals("Expected [VALUE_STRING] but found [VALUE_NULL] in [indices].", ex.getMessage());
        }
    }

    public void testParseXContentCustomIndexInvalidTypes() throws IOException {
        try (XContentParser content = createParser(XContentFactory.jsonBuilder()
        //tag::noformat
                .startObject()
                    .field("sample_size", 2000)
                    .field("indices", new int[] {1, 2, 3})
                    .field("stacktrace_ids_field", "stacktraces")
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
            assertEquals("Expected [VALUE_STRING] but found [VALUE_NUMBER] in [indices].", ex.getMessage());
        }
    }

    public void testValidateWrongSampleSize() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            randomIntBetween(Integer.MIN_VALUE, 0),
            1.0d,
            1.0d,
            1.0d,
            null,
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

    public void testValidateSampleSizeIsValidWithCustomIndices() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            10,
            1.0d,
            1.0d,
            1.0d,
            null,
            new String[] { randomAlphaOfLength(7) },
            randomAlphaOfLength(3),
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertNull("Expecting no validation errors", request.validate());
    }

    public void testValidateStacktraceWithoutIndices() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            1,
            1.0d,
            1.0d,
            1.0d,
            null,
            null,
            randomAlphaOfLength(3),
            null,
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[stacktrace_ids_field] must not be set", validationErrors.get(0));
    }

    public void testValidateIndicesWithoutStacktraces() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            null,
            new String[] { randomAlphaOfLength(5) },
            randomFrom("", null),
            null,
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[stacktrace_ids_field] is mandatory", validationErrors.get(0));
    }

    public void testValidateAggregationFieldsContainsTooFewElements() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            null,
            new String[] { randomAlphaOfLength(5) },
            randomAlphaOfLength(5),
            new String[] {},
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[aggregation_fields] must contain either one or two elements but contains [0] elements.", validationErrors.get(0));
    }

    public void testValidateAggregationFieldsContainsTooManyElements() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            null,
            new String[] { randomAlphaOfLength(5) },
            randomAlphaOfLength(5),
            new String[] { "application", "service", "transaction" },
            null,
            null,
            null,
            null,
            null
        );
        List<String> validationErrors = request.validate().validationErrors();
        assertEquals(1, validationErrors.size());
        assertEquals("[aggregation_fields] must contain either one or two elements but contains [3] elements.", validationErrors.get(0));
    }

    public void testValidateAggregationFieldsContainsEnoughElements() {
        GetStackTracesRequest request = new GetStackTracesRequest(
            null,
            1.0d,
            1.0d,
            1.0d,
            null,
            new String[] { randomAlphaOfLength(5) },
            randomAlphaOfLength(5),
            new String[] { "service", "service" },
            null,
            null,
            null,
            null,
            null
        );
        assertNull("Expecting no validation errors", request.validate());
    }

    public void testConsidersCustomIndicesInRelatedIndices() {
        String customIndex = randomAlphaOfLength(5);
        GetStackTracesRequest request = new GetStackTracesRequest(
            1,
            1.0d,
            1.0d,
            1.0d,
            null,
            new String[] { customIndex },
            randomAlphaOfLength(3),
            null,
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
        GetStackTracesRequest request = new GetStackTracesRequest(
            1,
            1.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
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
