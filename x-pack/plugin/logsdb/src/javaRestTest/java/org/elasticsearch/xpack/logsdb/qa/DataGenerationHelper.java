/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.DocumentGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.Mapping;
import org.elasticsearch.logsdb.datageneration.MappingGenerator;
import org.elasticsearch.logsdb.datageneration.Template;
import org.elasticsearch.logsdb.datageneration.TemplateGenerator;
import org.elasticsearch.logsdb.datageneration.fields.PredefinedField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DataGenerationHelper {
    private final boolean keepArraySource;

    private final DocumentGenerator documentGenerator;

    private final Template template;
    private final Mapping mapping;

    public DataGenerationHelper() {
        this(b -> {});
    }

    public DataGenerationHelper(Consumer<DataGeneratorSpecification.Builder> builderConfigurator) {
        this.keepArraySource = ESTestCase.randomBoolean();

        var specificationBuilder = DataGeneratorSpecification.builder()
            .withFullyDynamicMapping(ESTestCase.randomBoolean())
            .withPredefinedFields(
                List.of(
                    // Customized because it always needs doc_values for aggregations.
                    new PredefinedField.WithGenerator(
                        "host.name",
                        FieldType.KEYWORD,
                        Map.of("type", "keyword"),
                        () -> ESTestCase.randomAlphaOfLength(5)
                    ),
                    // Needed for terms query
                    new PredefinedField.WithGenerator(
                        "method",
                        FieldType.KEYWORD,
                        Map.of("type", "keyword"),
                        () -> ESTestCase.randomFrom("put", "post", "get")
                    ),

                    // Needed for histogram aggregation
                    new PredefinedField.WithGenerator(
                        "memory_usage_bytes",
                        FieldType.LONG,
                        Map.of("type", "long"),
                        () -> ESTestCase.randomLongBetween(1000, 2000)
                    )
                )
            );

        // Customize builder if necessary
        builderConfigurator.accept(specificationBuilder);

        var specification = specificationBuilder.build();

        this.documentGenerator = new DocumentGenerator(specification);

        this.template = new TemplateGenerator(specification).generate();
        this.mapping = new MappingGenerator(specification).generate(template);
    }

    void logsDbMapping(XContentBuilder builder) throws IOException {
        builder.map(mapping.raw());
    }

    void standardMapping(XContentBuilder builder) throws IOException {
        builder.map(mapping.raw());
    }

    void logsDbSettings(Settings.Builder builder) {
        if (keepArraySource) {
            builder.put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays");
        }
    }

    void generateDocument(XContentBuilder document, Map<String, Object> additionalFields) throws IOException {
        var generated = documentGenerator.generate(template, mapping);
        generated.putAll(additionalFields);

        document.map(generated);
    }
}
