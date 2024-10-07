/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.logsdb.datageneration.fields.DynamicMapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DocumentGenerator {
    private final DataGeneratorSpecification specification;

    private final DataSourceResponse.ObjectArrayGenerator objectArrayGenerator;

    public DocumentGenerator(DataGeneratorSpecification specification) {
        this.specification = specification;

        this.objectArrayGenerator = specification.dataSource().get(new DataSourceRequest.ObjectArrayGenerator());
    }

    public Map<String, Object> generate(MappingTemplate template, Mapping mapping) {
        var map = new HashMap<String, Object>();
        generateDocument(map, template.mapping(), new Context("", mapping.lookup()));
        return map;
    }

    private void generateDocument(Map<String, Object> document, Map<String, MappingTemplate.Entry> template, Context context) {
        for (var predefinedField : specification.predefinedFields()) {
            document.put(predefinedField.name(), predefinedField.generator(specification.dataSource()).generateValue());
        }

        for (var entry : template.entrySet()) {
            String fieldName = entry.getKey();
            MappingTemplate.Entry templateEntry = entry.getValue();

            if (templateEntry instanceof MappingTemplate.Entry.Leaf leaf) {
                // TODO remove this
                var mappingParametersGenerator = specification.dataSource()
                    .get(new DataSourceRequest.LeafMappingParametersGenerator(fieldName, leaf.type(), Set.of(), DynamicMapping.FORBIDDEN));

                // Unsigned long does not play well when dynamically mapped because
                // it gets mapped as just long and large values fail to index.
                // Just skip it.
                if (leaf.type() == FieldType.UNSIGNED_LONG && context.mappingLookup().get(context.pathTo(fieldName)) == null) {
                    continue;
                }

                var generator = leaf.type().generator(fieldName, specification.dataSource(), mappingParametersGenerator);

                document.put(fieldName, generator.generateValue());
            } else if (templateEntry instanceof MappingTemplate.Entry.Object object) {
                Optional<Integer> arrayLength = objectArrayGenerator.lengthGenerator().get();

                if (arrayLength.isPresent()) {
                    var children = new ArrayList<>(arrayLength.get());
                    document.put(object.name(), children);

                    for (int i = 0; i < arrayLength.get(); i++) {
                        children.add(generateObject(object, context));
                    }
                } else {
                    document.put(object.name(), generateObject(object, context));
                }
            }
        }
    }

    private Map<String, Object> generateObject(MappingTemplate.Entry.Object object, Context context) {
        var children = new HashMap<String, Object>();
        generateDocument(children, object.children(), context.stepIntoObject(object.name()));
        return children;
    }

    record Context(
        String path,
        Map<String, Map<String, Object>> mappingLookup
    ) {
        Context stepIntoObject(String name) {
            return new Context(pathTo(name), mappingLookup);
        }

        String pathTo(String leafFieldName) {
            return path.isEmpty() ? leafFieldName : path + "." + leafFieldName;
        }
    }
}
