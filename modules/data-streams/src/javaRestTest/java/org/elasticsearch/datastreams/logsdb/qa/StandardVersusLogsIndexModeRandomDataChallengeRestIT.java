/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.logsdb.datageneration.DataGenerator;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
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
    private final boolean keepArraySource;

    private final DataGenerator dataGenerator;

    public StandardVersusLogsIndexModeRandomDataChallengeRestIT() {
        super();
        // TODO enable subobjects: auto
        // It is disabled because it currently does not have auto flattening and that results in asserts being triggered when using copy_to.
        this.subobjects = randomValueOtherThan(ObjectMapper.Subobjects.AUTO, () -> randomFrom(ObjectMapper.Subobjects.values()));
        this.keepArraySource = randomBoolean();

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
                    // Customized because it always needs doc_values for aggregations.
                    new PredefinedField.WithGenerator("host.name", new FieldDataGenerator() {
                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
                            return b -> b.startObject().field("type", "keyword").endObject();
                        }

                        @Override
                        public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
                            return b -> b.value(randomAlphaOfLength(5));
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
    protected final Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            .put(org.elasticsearch.test.rest.ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "9000s")
            .build();
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
    public void contenderSettings(Settings.Builder builder) {
        if (keepArraySource) {
            builder.put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays");
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
