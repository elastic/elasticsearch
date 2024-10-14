/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TopLevelObjectFieldDataGenerator {
    private final Context context;
    private final Map<String, Object> mappingParameters;

    // Child fields of top level object that were explicitly requested, they have predefined name and type.
    private final List<GenericSubObjectFieldDataGenerator.ChildField> predefinedFields;
    // Child fields of top level object that are generated and merged with predefined fields.
    private final List<GenericSubObjectFieldDataGenerator.ChildField> generatedChildFields;

    public TopLevelObjectFieldDataGenerator(DataGeneratorSpecification specification) {
        DynamicMapping dynamicMapping;
        if (specification.fullyDynamicMapping()) {
            dynamicMapping = DynamicMapping.FORCED;
            this.mappingParameters = Map.of();
        } else {
            this.mappingParameters = new HashMap<>(
                // Value of subobjects here is for a parent of this object.
                // Since there is no parent we pass ENABLED to allow to set subobjects to any value at top level.
                specification.dataSource()
                    .get(new DataSourceRequest.ObjectMappingParametersGenerator(true, false, ObjectMapper.Subobjects.ENABLED))
                    .mappingGenerator()
                    .get()
            );
            // Top-level object can't be disabled because @timestamp is a required field in data streams.
            this.mappingParameters.remove("enabled");

            dynamicMapping = mappingParameters.getOrDefault("dynamic", "true").equals("strict")
                ? DynamicMapping.FORBIDDEN
                : DynamicMapping.SUPPORTED;
        }
        var subobjects = ObjectMapper.Subobjects.from(mappingParameters.getOrDefault("subobjects", "true"));

        // Value of subobjects here is for a parent of this object.
        // Since there is no parent we pass ENABLED to allow to set subobjects to any value at top level.
        this.context = new Context(specification, dynamicMapping, ObjectMapper.Subobjects.ENABLED);
        var genericGenerator = new GenericSubObjectFieldDataGenerator(context);

        this.predefinedFields = genericGenerator.generateChildFields(specification.predefinedFields());
        this.generatedChildFields = genericGenerator.generateChildFields(dynamicMapping, subobjects);
    }

    public CheckedConsumer<XContentBuilder, IOException> mappingWriter(Map<String, Object> customMappingParameters) {
        return b -> {
            b.startObject();

            var mergedParameters = Stream.of(this.mappingParameters, customMappingParameters)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (l, r) -> r));

            for (var entry : mergedParameters.entrySet()) {
                b.field(entry.getKey(), entry.getValue());
            }

            b.startObject("properties");
            GenericSubObjectFieldDataGenerator.writeChildFieldsMapping(b, predefinedFields);
            GenericSubObjectFieldDataGenerator.writeChildFieldsMapping(b, generatedChildFields);
            b.endObject();

            b.endObject();
        };
    }

    public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator(
        CheckedConsumer<XContentBuilder, IOException> customDocumentModification
    ) {
        CheckedConsumer<XContentBuilder, IOException> objectWriter = b -> {
            b.startObject();

            customDocumentModification.accept(b);
            GenericSubObjectFieldDataGenerator.writeChildFieldsData(b, predefinedFields);
            GenericSubObjectFieldDataGenerator.writeChildFieldsData(b, generatedChildFields);

            b.endObject();
        };
        return b -> GenericSubObjectFieldDataGenerator.writeObjectsData(b, context, objectWriter);
    }
}
