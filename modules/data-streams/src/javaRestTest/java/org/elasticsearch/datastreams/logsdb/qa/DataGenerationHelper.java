/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.logsdb.datageneration.DataGenerator;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.DocumentGenerator;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.Mapping;
import org.elasticsearch.logsdb.datageneration.MappingGenerator;
import org.elasticsearch.logsdb.datageneration.MappingTemplate;
import org.elasticsearch.logsdb.datageneration.MappingTemplateGenerator;
import org.elasticsearch.logsdb.datageneration.fields.PredefinedField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DataGenerationHelper {
    private final boolean keepArraySource;

    private final DocumentGenerator documentGenerator;

    private final MappingTemplate mappingTemplate;
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
                    new PredefinedField.WithGenerator("host.name", FieldType.KEYWORD, false, new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "keyword").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            return b -> b.value(ESTestCase.randomAlphaOfLength(5));
                        }

                        @Override
                        public Object generateValue() {
                            return ESTestCase.randomAlphaOfLength(5);
                        }
                    }),
                    // Needed for terms query
                    new PredefinedField.WithGenerator("method", FieldType.KEYWORD, false, new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "keyword").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            return b -> b.value(ESTestCase.randomFrom("put", "post", "get"));
                        }

                        @Override
                        public Object generateValue() {
                            return ESTestCase.randomFrom("put", "post", "get");
                        }
                    }),

                    // Needed for histogram aggregation
                    new PredefinedField.WithGenerator("memory_usage_bytes", FieldType.LONG, true, new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "long").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            // We can generate this using standard long field but we would get "too many buckets"
                            return b -> b.value(ESTestCase.randomLongBetween(1000, 2000));
                        }

                        @Override
                        public Object generateValue() {
                            return ESTestCase.randomLongBetween(1000, 2000);
                        }
                    })
                )
            );

        // Customize builder if necessary
        builderConfigurator.accept(specificationBuilder);

        var specification = specificationBuilder.build();

        this.documentGenerator = new DocumentGenerator(specification);

        this.mappingTemplate = new MappingTemplateGenerator(specification).generate();
        this.mapping = new MappingGenerator(specification).generate(mappingTemplate, null);
    }

    void logsDbMapping(XContentBuilder builder) throws IOException {
        builder.startObject().field("properties");
        builder.map(mapping.raw());
        builder.endObject();
    }

    void standardMapping(XContentBuilder builder) throws IOException {
        var rawMapping = new HashMap<>(mapping.raw());
        // We don't want standard to use custom subobjects values, this is not the goal of this test.
        // We want to compare "clean" standard with logsdb.
        removeSubobjects(rawMapping);

        builder.startObject().field("properties");
        builder.map(rawMapping);
        builder.endObject();
    }

    @SuppressWarnings("unchecked")
    private void removeSubobjects(Map<String, Object> mapping) {
        for (var entry : mapping.entrySet()) {
            if (entry.getValue() instanceof Map<?,?> m && m.containsKey("properties")) {
                m.remove("subobjects");
                removeSubobjects((Map<String, Object>) m.get("properties"));
            }
        }
    }

    void logsDbSettings(Settings.Builder builder) {
        if (keepArraySource) {
            builder.put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays");
        }
    }

    void generateDocument(XContentBuilder document, Map<String, Object> additionalFields)
        throws IOException {
        var generated = documentGenerator.generate(mappingTemplate, mapping);
        generated.putAll(additionalFields);

        document.map(generated);
    }
}
