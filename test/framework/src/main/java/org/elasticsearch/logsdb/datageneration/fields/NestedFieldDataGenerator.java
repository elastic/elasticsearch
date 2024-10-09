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
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NestedFieldDataGenerator implements FieldDataGenerator {
    private final Context context;
    private final Map<String, Object> mappingParameters;
    private final List<GenericSubObjectFieldDataGenerator.ChildField> childFields;

    NestedFieldDataGenerator(Context context) {
        this.context = context;

        this.mappingParameters = context.specification()
            .dataSource()
            .get(new DataSourceRequest.ObjectMappingParametersGenerator(false, true, context.getCurrentSubobjectsConfig()))
            .mappingGenerator()
            .get();
        var dynamicMapping = context.determineDynamicMapping(mappingParameters);
        var subobjects = context.determineSubobjects(mappingParameters);

        var genericGenerator = new GenericSubObjectFieldDataGenerator(context);
        this.childFields = genericGenerator.generateChildFields(dynamicMapping, subobjects);
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
        return b -> {
            b.startObject();

            b.field("type", "nested");

            for (var entry : mappingParameters.entrySet()) {
                b.field(entry.getKey(), entry.getValue());
            }

            b.startObject("properties");
            GenericSubObjectFieldDataGenerator.writeChildFieldsMapping(b, childFields);
            b.endObject();

            b.endObject();
        };
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator() {
        CheckedConsumer<XContentBuilder, IOException> objectWriter = object -> GenericSubObjectFieldDataGenerator.writeSingleObject(
            object,
            childFields
        );
        return b -> GenericSubObjectFieldDataGenerator.writeObjectsData(b, context, objectWriter);
    }
}
