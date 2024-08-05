/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class TopLevelObjectFieldDataGenerator {
    private final Context context;
    // Child fields of top level object that were explicitly requested, they have predefined name and type.
    private final List<GenericSubObjectFieldDataGenerator.ChildField> predefinedFields;
    // Child fields of top level object that are generated and merged with predefined fields.
    private final List<GenericSubObjectFieldDataGenerator.ChildField> generatedChildFields;

    public TopLevelObjectFieldDataGenerator(DataGeneratorSpecification specification) {
        this.context = new Context(specification);
        var genericGenerator = new GenericSubObjectFieldDataGenerator(context);
        this.predefinedFields = genericGenerator.generateChildFields(specification.predefinedFields());
        this.generatedChildFields = genericGenerator.generateChildFields();
    }

    public CheckedConsumer<XContentBuilder, IOException> mappingWriter(
        CheckedConsumer<XContentBuilder, IOException> customMappingParameters
    ) {
        return b -> {
            b.startObject();

            customMappingParameters.accept(b);

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
