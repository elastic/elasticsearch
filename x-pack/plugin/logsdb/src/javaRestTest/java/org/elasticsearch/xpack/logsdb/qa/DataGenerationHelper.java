/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.datageneration.fields.PredefinedField;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.datageneration.GeoShapeDataSourceHandler;
import org.elasticsearch.xpack.spatial.datageneration.ShapeDataSourceHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
                        (ignored) -> ESTestCase.randomAlphaOfLength(5)
                    ),
                    // Needed for terms query
                    new PredefinedField.WithGenerator(
                        "method",
                        FieldType.KEYWORD,
                        Map.of("type", "keyword"),
                        (ignored) -> ESTestCase.randomFrom("put", "post", "get")
                    ),

                    // Needed for histogram aggregation
                    new PredefinedField.WithGenerator(
                        "memory_usage_bytes",
                        FieldType.LONG,
                        Map.of("type", "long"),
                        (ignored) -> ESTestCase.randomLongBetween(1000, 2000)
                    )
                )
            )
            .withDataSourceHandlers(List.of(new GeoShapeDataSourceHandler(), new ShapeDataSourceHandler()))
            .withDataSourceHandlers(List.of(new DataSourceHandler() {
                @Override
                public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
                    return new DataSourceResponse.FieldTypeGenerator(new Supplier<>() {
                        // geo_shape and shape tends to produce really big values so let's limit how many we generate
                        private int shapesGenerated = 0;

                        @Override
                        public DataSourceResponse.FieldTypeGenerator.FieldTypeInfo get() {
                            // Base set of field types
                            var options = Arrays.stream(FieldType.values()).map(FieldType::toString).collect(Collectors.toSet());
                            // Custom types coming from specific functionality modules

                            if (shapesGenerated < 5) {
                                options.add("geo_shape");
                                options.add("shape");
                            }

                            var randomChoice = ESTestCase.randomFrom(options);
                            if (randomChoice.equals("geo_shape") || randomChoice.equals("shape")) {
                                shapesGenerated += 1;
                            }

                            return new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(ESTestCase.randomFrom(options));
                        }
                    });
                }
            }));

        // Customize builder if necessary
        builderConfigurator.accept(specificationBuilder);

        var specification = specificationBuilder.build();

        this.documentGenerator = new DocumentGenerator(specification);

        this.template = new TemplateGenerator(specification).generate();
        this.mapping = new MappingGenerator(specification).generate(template);
    }

    Mapping mapping() {
        return this.mapping;
    }

    void writeLogsDbMapping(XContentBuilder builder) throws IOException {
        builder.map(mapping.raw());
    }

    void writeStandardMapping(XContentBuilder builder) throws IOException {
        builder.map(mapping.raw());
    }

    void logsDbSettings(Settings.Builder builder) {
        builder.put("index.mode", "logsdb");
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
