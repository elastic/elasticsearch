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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MappingGenerator {
    private final DataGeneratorSpecification specification;

    private final DataSourceResponse.DynamicMappingGenerator dynamicMappingGenerator;

    public MappingGenerator(DataGeneratorSpecification specification) {
        this.specification = specification;

        this.dynamicMappingGenerator = specification.dataSource().get(new DataSourceRequest.DynamicMappingGenerator());
    }

    public Mapping generate(MappingTemplate template, Object someKindOfConfig) {
        var rawMapping = new HashMap<String, Object>();
        var lookup = new HashMap<String, Map<String, Object>>();

        generateMapping(
            rawMapping,
            lookup,
            template.mapping(),
            new Context(new HashSet<>(), "", ObjectMapper.Subobjects.ENABLED, DynamicMapping.SUPPORTED)
        );

        return new Mapping(rawMapping, lookup);
    }

    private void generateMapping(
        Map<String, Object> mapping,
        Map<String, Map<String, Object>> lookup,
        Map<String, MappingTemplate.Entry> mappingTemplate,
        Context context
    ) {
        // TODO handle top-level mapping parameters

        for (var entry : mappingTemplate.entrySet()) {
            String fieldName = entry.getKey();
            MappingTemplate.Entry templateEntry = entry.getValue();

            var mappingParameters = new HashMap<String, Object>();
            Boolean isDynamic = null;

            if (templateEntry instanceof MappingTemplate.Entry.Leaf leaf) {
                // For simplicity we only copy to keyword fields, synthetic source logic to handle copy_to is generic.
                if (leaf.type() == FieldType.KEYWORD) {
                    context.addCopyToCandidate(fieldName);
                }

                isDynamic = dynamicMappingGenerator.generator().apply(false);
                // Simply skip this field if it is dynamic.
                // Lookup will contain null signaling dynamic mapping as well.
                var mappingParametersGenerator = specification.dataSource()
                    .get(
                        new DataSourceRequest.LeafMappingParametersGenerator(
                            fieldName,
                            leaf.type(),
                            context.eligibleCopyToDestinations(),
                            context.parentDynamicMapping()
                        )
                    )
                    .mappingGenerator();

                mappingParameters.put("type", leaf.type().toString());
                mappingParameters.putAll(mappingParametersGenerator.get());

            } else if (templateEntry instanceof MappingTemplate.Entry.Object object) {
                isDynamic = dynamicMappingGenerator.generator().apply(false);

                var mappingParametersGenerator = specification.dataSource()
                    .get(new DataSourceRequest.ObjectMappingParametersGenerator(false, object.nested(), context.parentSubobjects()))
                    .mappingGenerator();

                mappingParameters.put("type", object.nested() ? "nested" : "object");
                mappingParameters.putAll(mappingParametersGenerator.get());

                var childrenMapping = new HashMap<String, Object>();
                mappingParameters.put("properties", childrenMapping);
                generateMapping(childrenMapping, lookup, object.children(), context.stepIntoObject(object.name(), mappingParameters));
            }

            // Simply skip this field if it is dynamic.
            // Lookup will contain null signaling dynamic mapping as well.
            if (isDynamic == false) {
                mapping.put(fieldName, mappingParameters);
                lookup.put(context.pathTo(fieldName), Map.copyOf(mappingParameters));
            }
        }
    }

    record Context(
        Set<String> eligibleCopyToDestinations,
        String path,
        ObjectMapper.Subobjects parentSubobjects,
        DynamicMapping parentDynamicMapping
    ) {
        Context stepIntoObject(String name, Map<String, Object> mappingParameters) {
            var subobjects = determineSubobjects(mappingParameters);
            var dynamicMapping = determineDynamicMapping(mappingParameters);

            return new Context(eligibleCopyToDestinations, pathTo(name), subobjects, dynamicMapping);
        }

        void addCopyToCandidate(String field) {
            eligibleCopyToDestinations.add(field);
        }

        String pathTo(String leafFieldName) {
            return path.isEmpty() ? leafFieldName : path + "." + leafFieldName;
        }

        private DynamicMapping determineDynamicMapping(Map<String, Object> mappingParameters) {
            if (parentDynamicMapping == DynamicMapping.FORCED) {
                return DynamicMapping.FORCED;
            }

            var dynamicParameter = mappingParameters.get("dynamic");
            // Inherited from parent
            if (dynamicParameter == null) {
                return parentDynamicMapping;
            }

            return dynamicParameter.equals("strict") ? DynamicMapping.FORBIDDEN : DynamicMapping.SUPPORTED;
        }

        private ObjectMapper.Subobjects determineSubobjects(Map<String, Object> mappingParameters) {
            if (parentSubobjects == ObjectMapper.Subobjects.DISABLED) {
                return ObjectMapper.Subobjects.DISABLED;
            }

            return ObjectMapper.Subobjects.from(mappingParameters.getOrDefault("subobjects", "true"));
        }
    }
}
