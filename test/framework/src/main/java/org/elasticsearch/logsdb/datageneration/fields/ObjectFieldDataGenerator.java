/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.fields;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.logsdb.datageneration.FieldDataGenerator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class ObjectFieldDataGenerator implements FieldDataGenerator {
    private final Context context;
    private final List<GenericSubObjectFieldDataGenerator.ChildField> childFields;

    ObjectFieldDataGenerator(Context context) {
        this.context = context;
        var genericGenerator = new GenericSubObjectFieldDataGenerator(context);
        this.childFields = genericGenerator.generateChildFields();
    }

    @Override
    public CheckedConsumer<XContentBuilder, IOException> mappingWriter() {
        return b -> {
            b.startObject();

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
