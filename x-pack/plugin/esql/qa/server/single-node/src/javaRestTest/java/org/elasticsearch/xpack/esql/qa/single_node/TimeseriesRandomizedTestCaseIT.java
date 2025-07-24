/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.fields.PredefinedField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeseriesRandomizedTestCaseIT extends GenerativeIT {

    private static final int MAX_DEPTH = 15;

    @Test
    public void testRandomTimeseriesQueries() throws Exception {
        String indexName = "timeseries_test";
        generateAndIndexData(indexName, 200);

        // Ten random queries
        for (int i = 0; i < 10; i++) {
            runRandomQuery(indexName);
        }
    }

    private void runRandomQuery(String indexName) {
        CommandGenerator.QuerySchema mappingInfo = new CommandGenerator.QuerySchema(List.of(indexName), List.of(), List.of());
        List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();

        // Base query to select recent data
        String baseQuery = "FROM " + indexName + " | WHERE @timestamp > NOW() - INTERVAL 1 DAY";
        EsqlQueryResponse response = run(baseQuery);
        List<EsqlQueryGenerator.Column> currentSchema = toGeneratorSchema(response.columns());

        String currentQuery = baseQuery;

        for (int j = 0; j < MAX_DEPTH; j++) {
            if (currentSchema.isEmpty()) {
                break;
            }

            CommandGenerator commandGenerator = EsqlQueryGenerator.randomPipeCommandGenerator();
            CommandGenerator.CommandDescription desc = commandGenerator.generate(previousCommands, currentSchema, mappingInfo);

            if (desc == CommandGenerator.EMPTY_DESCRIPTION) {
                continue;
            }

            String nextCommand = desc.commandString();
            currentQuery += nextCommand;

            try {
                response = run(currentQuery);
                currentSchema = toGeneratorSchema(response.columns());
                previousCommands.add(desc);
            } catch (Exception e) {
                // For now, we fail on any exception. More sophisticated error handling could be added here.
                throw new AssertionError("Query failed: " + currentQuery, e);
            }
        }
    }

    private DataGeneratorSpecification createDataSpec() {
        return DataGeneratorSpecification.builder()
            .withMaxFieldCountPerLevel(10)
            .withMaxObjectDepth(2)
            .withPredefinedFields(List.of(new PredefinedField.WithGenerator(
                "timestamp",
                FieldType.DATE,
                Map.of("type", "date"),
                (ignored) -> ESTestCase.randomPositiveTimeValue()
            )))
            .build();
    }

    private void generateAndIndexData(String indexName, int numDocs) throws Exception {
        var spec = createDataSpec();
        DocumentGenerator dataGenerator = new DocumentGenerator(spec);
        var templateGen = new TemplateGenerator(spec).generate();
        var mappingGen = new MappingGenerator(spec).generate(templateGen);
        // Create a new index with a randomized mapping
        createIndex(indexName, Settings.EMPTY);

        BulkRequest bulkRequest = new BulkRequest(indexName);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> document = dataGenerator.generate(templateGen, mappingGen);
            bulkRequest.add(new IndexRequest().source(document, XContentType.JSON));
        }
        client().bulk(bulkRequest).actionGet();
        refresh(indexName);
    }

    private List<EsqlQueryGenerator.Column> toGeneratorSchema(List<ColumnInfoImpl> columnInfos) {
        if (columnInfos == null) {
            return List.of();
        }
        return columnInfos.stream()
            .map(ci -> new EsqlQueryGenerator.Column(ci.name(), ci.outputType()))
            .collect(Collectors.toList());
    }
}
