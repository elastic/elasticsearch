/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.fields.PredefinedField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TimeseriesDataGenerationHelper {

    private final DocumentGenerator documentGenerator;
    private final Template template;
    private final Mapping mapping;

    public TimeseriesDataGenerationHelper() {
        this(b -> {});
    }

    public TimeseriesDataGenerationHelper(Consumer<DataGeneratorSpecification.Builder> builderConfigurator) {
        var specificationBuilder = DataGeneratorSpecification.builder()
            .withFullyDynamicMapping(ESTestCase.randomBoolean())
            .withPredefinedFields(
                List.of(
                    new PredefinedField.WithGenerator(
                        "@timestamp",
                        FieldType.DATE,
                        Map.of("type", "date"),
                        (ignored) -> Instant.now().toEpochMilli() - ESTestCase.randomLongBetween(0, 100000)
                    ),
                    new PredefinedField.WithGenerator(
                        "metric",
                        FieldType.DOUBLE,
                        Map.of("type", "double"),
                        (ignored) -> ESTestCase.randomDouble() * 100
                    ),
                    new PredefinedField.WithGenerator(
                        "labels.host",
                        FieldType.KEYWORD,
                        Map.of("type", "keyword"),
                        (ignored) -> "host-" + ESTestCase.randomIntBetween(1, 10)
                    ),
                    new PredefinedField.WithGenerator(
                        "labels.service",
                        FieldType.KEYWORD,
                        Map.of("type", "keyword"),
                        (ignored) -> "service-" + ESTestCase.randomAlphaOfLength(5)
                    )
                )
            );

        builderConfigurator.accept(specificationBuilder);
        var specification = specificationBuilder.build();

        this.documentGenerator = new DocumentGenerator(specification);
        this.template = new TemplateGenerator(specification).generate();
        this.mapping = new MappingGenerator(specification).generate(template);
    }

    public Mapping getMapping() {
        return this.mapping;
    }

    public void writeMapping(XContentBuilder builder) throws IOException {
        builder.map(mapping.raw());
    }

    public void generateDocument(XContentBuilder document) throws IOException {
        var generated = documentGenerator.generate(template, mapping);
        document.map(generated);
    }

    public String generateDocumentAsString() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        generateDocument(builder);
        return builder.toString();
    }
}
