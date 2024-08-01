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
import org.elasticsearch.logsdb.datageneration.DataGenerator;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.FieldType;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.logsdb.datageneration.fields.PredefinedField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;

/**
 * Challenge test (see {@link StandardVersusLogsIndexModeChallengeRestIT}) that uses randomly generated
 * mapping and documents in order to cover more code paths and permutations.
 */
public class StandardVersusLogsIndexModeRandomDataChallengeRestIT extends StandardVersusLogsIndexModeChallengeRestIT {
    private final boolean fullyDynamicMapping;

    private final DataGenerator dataGenerator;

    public StandardVersusLogsIndexModeRandomDataChallengeRestIT() {
        super();
        this.fullyDynamicMapping = randomBoolean();

        this.dataGenerator = new DataGenerator(
            DataGeneratorSpecification.builder()
                // Nested fields don't work with subobjects: false.
                .withNestedFieldsLimit(0)
                // TODO increase depth of objects
                // Currently matching fails because in synthetic source all fields are flat (given that we have subobjects: false)
                // but stored source is identical to original document which has nested structure.
                .withMaxObjectDepth(0)
                .withDataSourceHandlers(List.of(new DataSourceHandler() {
                    // TODO enable null values
                    // Matcher does not handle nulls currently
                    @Override
                    public DataSourceResponse.NullWrapper handle(DataSourceRequest.NullWrapper request) {
                        return new DataSourceResponse.NullWrapper(Function.identity());
                    }

                    // TODO enable arrays
                    // List matcher currently does not apply matching logic recursively
                    // and equality check fails because arrays are sorted in synthetic source.
                    @Override
                    public DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper request) {
                        return new DataSourceResponse.ArrayWrapper(Function.identity());
                    }

                    // TODO enable scaled_float fields
                    // There a difference in synthetic source (precision loss)
                    // specific to this fields which matcher can't handle.
                    @Override
                    public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
                        // Unsigned long is not used with dynamic mapping
                        // since it can initially look like long
                        // but later fail to parse once big values arrive.
                        // Double is not used since it maps to float with dynamic mapping
                        // resulting in precision loss compared to original source.
                        var excluded = fullyDynamicMapping
                            ? List.of(FieldType.DOUBLE, FieldType.SCALED_FLOAT, FieldType.UNSIGNED_LONG)
                            : List.of(FieldType.SCALED_FLOAT);
                        return new DataSourceResponse.FieldTypeGenerator(
                            () -> randomValueOtherThanMany(excluded::contains, () -> randomFrom(FieldType.values()))
                        );
                    }
                }))
                .withPredefinedFields(List.of(new PredefinedField("host.name", FieldType.KEYWORD)))
                .build()
        );
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
            dataGenerator.writeMapping(builder, b -> builder.field("subobjects", false));
        } else {
            builder.startObject();
            builder.field("subobjects", false);
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
