/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.profileschema;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.MMROperator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.test.xcontent.JsonSchemaTestValidator;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.EsqlQueryProfile;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.orc.OrcDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlProfileSchemaRegistry;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EsqlProfileSchemaDocumentTests {

    @Test
    public void testGeneratedDocumentIsDeterministicAndComplete() {
        Map<String, Object> first = EsqlProfileSchemaDocument.create();
        assertEquals(first, EsqlProfileSchemaDocument.create());

        Map<String, Object> definitions = object(first.get("definitions"));
        assertNotNull(definitions.get("EsqlProfile"));
        assertNotNull(definitions.get("OperatorStatus"));
        assertNotNull(definitions.get("FormatReaderStatus"));
        for (var variant : EsqlProfileSchemaRegistry.operatorStatusVariants()) {
            assertTrue(definitions.containsKey("OperatorStatus_" + variant.name().replaceAll("[^A-Za-z0-9_]", "_")));
        }
        CsvDataSourcePlugin csv = new CsvDataSourcePlugin();
        assertFormatRegistryMatches(csv.getNamedWriteables(), csv);
        NdJsonDataSourcePlugin ndjson = new NdJsonDataSourcePlugin();
        assertFormatRegistryMatches(ndjson.getNamedWriteables(), ndjson);
        OrcDataSourcePlugin orc = new OrcDataSourcePlugin();
        assertFormatRegistryMatches(orc.getNamedWriteables(), orc);
        ParquetDataSourcePlugin parquet = new ParquetDataSourcePlugin();
        assertFormatRegistryMatches(parquet.getNamedWriteables(), parquet);
    }

    @Test
    public void testGeneratedDocumentReusesDefinitions() {
        Map<String, Object> definitions = object(EsqlProfileSchemaDocument.create().get("definitions"));
        Map<String, Object> profileProperties = object(object(definitions.get("EsqlProfile")).get("properties"));
        Map<String, Object> nullableTimeSpan = ProfileSchema.nullable(ProfileSchema.ref("TimeSpan"));
        for (String property : List.of(
            EsqlQueryProfile.QUERY,
            EsqlQueryProfile.PLANNING,
            EsqlQueryProfile.PARSING,
            EsqlQueryProfile.VIEW_RESOLUTION,
            EsqlQueryProfile.DATASET_RESOLUTION,
            EsqlQueryProfile.PRE_ANALYSIS,
            EsqlQueryProfile.INDICES_RESOLUTION,
            EsqlQueryProfile.ENRICH_RESOLUTION,
            EsqlQueryProfile.INFERENCE_RESOLUTION,
            EsqlQueryProfile.ANALYSIS
        )) {
            assertEquals(nullableTimeSpan, profileProperties.get(property));
        }

        Map<String, Object> driverProperties = object(object(definitions.get("DriverProfile")).get("properties"));
        assertEquals(ProfileSchema.ref("DriverSleeps"), driverProperties.get("sleeps"));

        Map<String, Object> sleepsProperties = object(object(definitions.get("DriverSleeps")).get("properties"));
        Map<String, Object> sleepArray = ProfileSchema.array(ProfileSchema.ref("DriverSleep"));
        assertEquals(sleepArray, sleepsProperties.get("first"));
        assertEquals(sleepArray, sleepsProperties.get("last"));

        List<?> operatorAlternatives = (List<?>) object(definitions.get("OperatorStatus")).get("anyOf");
        assertEquals(2, operatorAlternatives.size());
        Map<String, Object> withStatusProperties = object(object(operatorAlternatives.get(1)).get("properties"));
        List<?> statusAlternatives = (List<?>) object(withStatusProperties.get("status")).get("anyOf");
        assertEquals(EsqlProfileSchemaRegistry.operatorStatusVariants().size(), statusAlternatives.size());
    }

    @Test
    public void testRepresentativeProfileValidates() throws IOException {
        JsonSchemaTestValidator validator = JsonSchemaTestValidator.draft7(EsqlProfileSchemaDocument.create());

        DriverSleeps.Sleep sleep = new DriverSleeps.Sleep("exchange empty", "worker-1", 1, 2);
        DriverProfile driver = new DriverProfile(
            "data",
            "cluster",
            "node",
            1,
            2,
            1_000_000,
            100,
            200,
            1,
            List.of(new OperatorStatus("MMR", new MMROperator.Status(1, 1, 1, 1, 1)), new OperatorStatus("Limit", null)),
            new DriverSleeps(Map.of("exchange empty", 1L), List.of(sleep), List.of(sleep))
        );
        PlanProfile plan = new PlanProfile("data", "cluster", "node", "Limit[10]", "Limit[10]", new PlanTimeProfile(1, 2, 3));
        var value = new EsqlQueryResponse.ProfileValue(
            new EsqlQueryResponse.Profile(List.of(driver), List.of(plan), null),
            new EsqlQueryProfile()
        );

        assertValid(validator, value, false);
        assertValid(validator, value, true);
    }

    private static void assertValid(JsonSchemaTestValidator validator, EsqlQueryResponse.ProfileValue value, boolean humanReadable)
        throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().humanReadable(humanReadable)) {
            EsqlQueryResponse.PROFILE_SCHEMA.write(builder, value, ToXContent.EMPTY_PARAMS);
            assertEquals(List.of(), validator.validate(BytesReference.bytes(builder)));
        }
    }

    private static void assertFormatRegistryMatches(List<NamedWriteableRegistry.Entry> registrations, DataSourcePlugin plugin) {
        assertEquals(
            registrations.stream().map(entry -> entry.name).toList(),
            plugin.formatReaderProfileVariants().stream().map(variant -> variant.name()).toList()
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }
}
