/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration;

import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Generator that generates a valid random document that follows the structure of provided {@link Template}.
 */
public class DocumentGenerator {
    private final DataGeneratorSpecification specification;

    private final DataSourceResponse.ObjectArrayGenerator objectArrayGenerator;

    public DocumentGenerator(DataGeneratorSpecification specification) {
        this.specification = specification;

        this.objectArrayGenerator = specification.dataSource().get(new DataSourceRequest.ObjectArrayGenerator());
    }

    /**
     * Generates a valid random document following the provided template.
     * @param template template for the document
     * @param mapping generated mapping that will be applied to the destination index of this document
     * @return document as a map where subobjects are represented as nested maps
     */
    public Map<String, Object> generate(Template template, Mapping mapping) {
        var documentMap = new TreeMap<String, Object>();
        for (var predefinedField : specification.predefinedFields()) {
            documentMap.put(
                predefinedField.name(),
                predefinedField.generator(specification.dataSource()).generateValue(predefinedField.mapping())
            );
        }

        generateFields(documentMap, template.template(), new Context("", mapping.lookup()));
        return documentMap;
    }

    private void generateFields(Map<String, Object> document, Map<String, Template.Entry> template, Context context) {
        for (var entry : template.entrySet()) {
            String fieldName = entry.getKey();
            Template.Entry templateEntry = entry.getValue();

            if (templateEntry instanceof Template.Leaf leaf) {
                var fieldMapping = context.mappingLookup().get(context.pathTo(fieldName));

                // Unsigned long does not play well when dynamically mapped because
                // it gets mapped as just long and large values fail to index.
                // Just skip it.
                // TODO we can actually handle this in UnsignedLongFieldDataGenerator
                if (leaf.type().equals(FieldType.UNSIGNED_LONG.toString()) && fieldMapping == null) {
                    continue;
                }

                var generator = specification.dataSource()
                    .get(new DataSourceRequest.FieldDataGenerator(fieldName, leaf.type(), specification.dataSource()))
                    .generator();
                document.put(fieldName, generator.generateValue(fieldMapping));
            } else if (templateEntry instanceof Template.Object object) {
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

    private Map<String, Object> generateObject(Template.Object object, Context context) {
        var children = new TreeMap<String, Object>();
        generateFields(children, object.children(), context.stepIntoObject(object.name()));
        return children;
    }

    record Context(String path, Map<String, Map<String, Object>> mappingLookup) {
        Context stepIntoObject(String name) {
            return new Context(pathTo(name), mappingLookup);
        }

        String pathTo(String leafFieldName) {
            return path.isEmpty() ? leafFieldName : path + "." + leafFieldName;
        }
    }
}
