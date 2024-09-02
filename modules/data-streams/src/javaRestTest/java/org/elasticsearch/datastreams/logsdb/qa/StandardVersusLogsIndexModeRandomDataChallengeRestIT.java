/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.logsdb.datageneration.DataGenerator;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.logsdb.datageneration.fields.PredefinedField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Challenge test (see {@link StandardVersusLogsIndexModeChallengeRestIT}) that uses randomly generated
 * mapping and documents in order to cover more code paths and permutations.
 */
public class StandardVersusLogsIndexModeRandomDataChallengeRestIT extends StandardVersusLogsIndexModeChallengeRestIT {
    private final ObjectMapper.Subobjects subobjects;

    private final DataGenerator dataGenerator;

    public StandardVersusLogsIndexModeRandomDataChallengeRestIT() {
        super();
        this.subobjects = randomFrom(ObjectMapper.Subobjects.values());

        var specificationBuilder = DataGeneratorSpecification.builder().withFullyDynamicMapping(randomBoolean());
        if (subobjects != ObjectMapper.Subobjects.ENABLED) {
            specificationBuilder = specificationBuilder.withNestedFieldsLimit(0);
        }
        this.dataGenerator = new DataGenerator(specificationBuilder.withDataSourceHandlers(List.of(new DataSourceHandler() {
            @Override
            public DataSourceResponse.ObjectMappingParametersGenerator handle(DataSourceRequest.ObjectMappingParametersGenerator request) {
                if (subobjects == ObjectMapper.Subobjects.ENABLED) {
                    // Use default behavior
                    return null;
                }

                assert request.isNested() == false;

                // "enabled: false" is not compatible with subobjects: false
                // "dynamic: false/strict/runtime" is not compatible with subobjects: false
                return new DataSourceResponse.ObjectMappingParametersGenerator(() -> {
                    var parameters = new HashMap<String, Object>();
                    parameters.put("subobjects", subobjects.toString());
                    if (ESTestCase.randomBoolean()) {
                        parameters.put("dynamic", "true");
                    }
                    if (ESTestCase.randomBoolean()) {
                        parameters.put("enabled", "true");
                    }
                    return parameters;
                });
            }
        }))
            .withPredefinedFields(
                List.of(
                    new PredefinedField.WithType("host.name", FieldType.KEYWORD),
                    // Needed for terms query
                    new PredefinedField.WithGenerator("method", new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "keyword").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            return b -> b.value(randomFrom("put", "post", "get"));
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
                            return b -> b.value(randomLongBetween(1000, 2000));
                        }
                    })
                )
            )
            .build());
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        dataGenerator.writeMapping(builder);
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        if (subobjects != ObjectMapper.Subobjects.ENABLED) {
            dataGenerator.writeMapping(builder, Map.of("subobjects", subobjects.toString()));
        } else {
            dataGenerator.writeMapping(builder);
        }
    }

    @Override
    protected XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        var document = XContentFactory.jsonBuilder();
        dataGenerator.generateDocument(document, doc -> {
            doc.field("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp));
        });

        return document;
    }
}
