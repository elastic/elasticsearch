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
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.fields.PredefinedField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class DataGenerationHelper {
    private final boolean keepArraySource;

    private final DataGenerator dataGenerator;

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
                    new PredefinedField.WithGenerator("host.name", new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "keyword").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            return b -> b.value(ESTestCase.randomAlphaOfLength(5));
                        }
                    }),
                    // Needed for terms query
                    new PredefinedField.WithGenerator("method", new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "keyword").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            return b -> b.value(ESTestCase.randomFrom("put", "post", "get"));
                        }
                    }),

                    // Needed for histogram aggregation
                    new PredefinedField.WithGenerator("memory_usage_bytes", new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "long").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            // We can generate this using standard long field but we would get "too many buckets"
                            return b -> b.value(ESTestCase.randomLongBetween(1000, 2000));
                        }
                    })
                )
            );

        // Customize builder if necessary
        builderConfigurator.accept(specificationBuilder);

        this.dataGenerator = new DataGenerator(specificationBuilder.build());
    }

    DataGenerator getDataGenerator() {
        return dataGenerator;
    }

    void logsDbMapping(XContentBuilder builder) throws IOException {
        dataGenerator.writeMapping(builder);
    }

    void standardMapping(XContentBuilder builder) throws IOException {
        dataGenerator.writeMapping(builder);
    }

    void logsDbSettings(Settings.Builder builder) {
        if (keepArraySource) {
            builder.put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays");
        }
    }
}
