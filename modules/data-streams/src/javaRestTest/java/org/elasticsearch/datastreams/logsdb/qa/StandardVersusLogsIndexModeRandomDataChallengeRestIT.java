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
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.logsdb.datageneration.DataGenerator;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
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

/**
 * Challenge test (see {@link StandardVersusLogsIndexModeChallengeRestIT}) that uses randomly generated
 * mapping and documents in order to cover more code paths and permutations.
 */
public class StandardVersusLogsIndexModeRandomDataChallengeRestIT extends StandardVersusLogsIndexModeChallengeRestIT {
    private final boolean fullyDynamicMapping;
    private final ObjectMapper.Subobjects subobjects;

    private final DataGenerator dataGenerator;

    public StandardVersusLogsIndexModeRandomDataChallengeRestIT() {
        super();
        this.fullyDynamicMapping = randomBoolean();
        this.subobjects = randomFrom(ObjectMapper.Subobjects.values());

        var specificationBuilder = DataGeneratorSpecification.builder();
        if (subobjects != ObjectMapper.Subobjects.ENABLED) {
            specificationBuilder = specificationBuilder.withNestedFieldsLimit(0);
        }
        this.dataGenerator = new DataGenerator(specificationBuilder.withDataSourceHandlers(List.of(new DataSourceHandler() {
            @Override
            public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
                // Unsigned long is not used with dynamic mapping
                // since it can initially look like long
                // but later fail to parse once big values arrive.
                // Double is not used since it maps to float with dynamic mapping
                // resulting in precision loss compared to original source.
                var excluded = fullyDynamicMapping ? List.of(FieldType.DOUBLE, FieldType.SCALED_FLOAT, FieldType.UNSIGNED_LONG) : List.of();
                return new DataSourceResponse.FieldTypeGenerator(
                    () -> randomValueOtherThanMany(excluded::contains, () -> randomFrom(FieldType.values()))
                );
            }

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
        })).withPredefinedFields(List.of(new PredefinedField("host.name", FieldType.KEYWORD))).build());
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        if (fullyDynamicMapping == false) {
            dataGenerator.writeMapping(builder);
        } else {
            // We want dynamic mapping, but we need host.name to be a keyword instead of text to support aggregations.
            builder.startObject()
                .startObject("properties")

                .startObject("host.name")
                .field("type", "keyword")
                .field("ignore_above", randomIntBetween(1000, 1200))
                .endObject()

                .endObject()
                .endObject();
        }
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        if (fullyDynamicMapping == false) {
            if (subobjects != ObjectMapper.Subobjects.ENABLED) {
                dataGenerator.writeMapping(builder, b -> builder.field("subobjects", subobjects.toString()));
            } else {
                dataGenerator.writeMapping(builder);
            }
        } else {
            builder.startObject();
            if (subobjects != ObjectMapper.Subobjects.ENABLED) {
                builder.field("subobjects", subobjects.toString());
            }
            builder.endObject();
        }
    }

    @Override
    protected XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        var document = XContentFactory.jsonBuilder();
        dataGenerator.generateDocument(document, doc -> {
            doc.field("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp));
            // Needed for terms query
            doc.field("method", randomFrom("put", "post", "get"));
            // We can generate this but we would get "too many buckets"
            doc.field("memory_usage_bytes", randomLongBetween(1000, 2000));
        });

        return document;
    }
}
