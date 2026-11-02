/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.profileschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testGeneratedDocumentIsDeterministicAndComplete() {
        Map<String, Object> first = EsqlProfileSchemaDocument.create();
        assertEquals(first, EsqlProfileSchemaDocument.create());

        Map<?, ?> definitions = (Map<?, ?>) first.get("definitions");
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
    public void testRepresentativeProfileValidates() throws IOException {
        JsonNode schemaNode = OBJECT_MAPPER.valueToTree(EsqlProfileSchemaDocument.create());
        JsonSchema schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaNode);

        DriverProfile driver = new DriverProfile("data", "cluster", "node", 1, 2, 1_000_000, 100, 200, 1, List.of(), DriverSleeps.empty());
        var value = new EsqlQueryResponse.ProfileValue(
            new EsqlQueryResponse.Profile(List.of(driver), List.of(), null),
            new EsqlQueryProfile()
        );

        assertValid(schema, value, false);
        assertValid(schema, value, true);
    }

    private static void assertValid(JsonSchema schema, EsqlQueryResponse.ProfileValue value, boolean humanReadable) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().humanReadable(humanReadable)) {
            EsqlQueryResponse.PROFILE_SCHEMA.write(builder, value, ToXContent.EMPTY_PARAMS);
            JsonNode profile = OBJECT_MAPPER.readTree(Strings.toString(builder));
            assertEquals(List.of(), List.copyOf(schema.validate(profile)));
        }
    }

    private static void assertFormatRegistryMatches(List<NamedWriteableRegistry.Entry> registrations, DataSourcePlugin plugin) {
        assertEquals(
            registrations.stream().map(entry -> entry.name).toList(),
            plugin.formatReaderProfileVariants().stream().map(variant -> variant.name()).toList()
        );
    }
}
