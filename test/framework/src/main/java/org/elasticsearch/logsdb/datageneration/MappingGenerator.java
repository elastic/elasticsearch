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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Generator that generates a valid random mapping that follows the structure of provided {@link Template}.
 * Mapping will contain all fields from the template with generated mapping parameters.
 */
public class MappingGenerator {
    private final DataGeneratorSpecification specification;

    private final DataSourceResponse.DynamicMappingGenerator dynamicMappingGenerator;

    public MappingGenerator(DataGeneratorSpecification specification) {
        this.specification = specification;

        this.dynamicMappingGenerator = specification.dataSource().get(new DataSourceRequest.DynamicMappingGenerator());
    }

    /**
     * Generates a valid random mapping following the provided template.
     * @param template template for the mapping
     * @return {@link Mapping}
     */
    public Mapping generate(Template template) {
        var lookup = new TreeMap<String, Map<String, Object>>();

        // Top level mapping parameters
        var mappingParametersGenerator = specification.dataSource()
            .get(new DataSourceRequest.ObjectMappingParametersGenerator(true, false, ObjectMapper.Subobjects.ENABLED))
            .mappingGenerator();

        var topLevelMappingParameters = mappingParametersGenerator.get();
        // Top-level object can't be disabled because @timestamp is a required field in data streams.
        topLevelMappingParameters.remove("enabled");

        var rawMapping = new TreeMap<String, Object>();

        var childrenMapping = new TreeMap<String, Object>();
        for (var predefinedField : specification.predefinedFields()) {
            if (predefinedField.mapping() != null) {
                childrenMapping.put(predefinedField.name(), predefinedField.mapping());
                lookup.put(predefinedField.name(), predefinedField.mapping());
            }
        }
        topLevelMappingParameters.put("properties", childrenMapping);

        rawMapping.put("_doc", topLevelMappingParameters);

        if (specification.fullyDynamicMapping()) {
            // Has to be "true" for fully dynamic mapping
            topLevelMappingParameters.remove("dynamic");

            return new Mapping(rawMapping, lookup);
        }

        var dynamicMapping = topLevelMappingParameters.getOrDefault("dynamic", "true").equals("strict")
            ? DynamicMapping.FORBIDDEN
            : DynamicMapping.SUPPORTED;
        var subobjects = ObjectMapper.Subobjects.from(topLevelMappingParameters.getOrDefault("subobjects", "true"));

        generateMapping(childrenMapping, lookup, template.template(), new Context(new HashSet<>(), "", subobjects, dynamicMapping));

        return new Mapping(rawMapping, lookup);
    }

    private void generateMapping(
        Map<String, Object> mapping,
        Map<String, Map<String, Object>> lookup,
        Map<String, Template.Entry> template,
        Context context
    ) {
        for (var entry : template.entrySet()) {
            String fieldName = entry.getKey();
            Template.Entry templateEntry = entry.getValue();

            var mappingParameters = new TreeMap<String, Object>();

            boolean isDynamicallyMapped = isDynamicallyMapped(templateEntry, context);
            // Simply skip this field if it is dynamic.
            // Lookup will contain null signaling dynamic mapping as well.
            if (isDynamicallyMapped) {
                continue;
            }

            if (templateEntry instanceof Template.Leaf leaf) {
                // For simplicity we only copy to keyword fields, synthetic source logic to handle copy_to is generic.
                if (leaf.type() == FieldType.KEYWORD) {
                    context.addCopyToCandidate(fieldName);
                }

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

            } else if (templateEntry instanceof Template.Object object) {
                var mappingParametersGenerator = specification.dataSource()
                    .get(new DataSourceRequest.ObjectMappingParametersGenerator(false, object.nested(), context.parentSubobjects()))
                    .mappingGenerator();

                mappingParameters.put("type", object.nested() ? "nested" : "object");
                mappingParameters.putAll(mappingParametersGenerator.get());

                var childrenMapping = new TreeMap<String, Object>();
                mappingParameters.put("properties", childrenMapping);
                generateMapping(
                    childrenMapping,
                    lookup,
                    object.children(),
                    context.stepIntoObject(object.name(), object.nested(), mappingParameters)
                );
            }

            mapping.put(fieldName, mappingParameters);
            lookup.put(context.pathTo(fieldName), Map.copyOf(mappingParameters));
        }
    }

    private boolean isDynamicallyMapped(Template.Entry templateEntry, Context context) {
        return context.parentDynamicMapping != DynamicMapping.FORBIDDEN
            && dynamicMappingGenerator.generator().apply(templateEntry instanceof Template.Object);
    }

    record Context(
        Set<String> eligibleCopyToDestinations,
        String path,
        ObjectMapper.Subobjects parentSubobjects,
        DynamicMapping parentDynamicMapping
    ) {
        Context stepIntoObject(String name, boolean nested, Map<String, Object> mappingParameters) {
            var subobjects = determineSubobjects(mappingParameters);
            var dynamicMapping = determineDynamicMapping(mappingParameters);

            // copy_to can't be used across nested documents so all currently eligible fields are not eligible inside nested document.
            return new Context(nested ? new HashSet<>() : eligibleCopyToDestinations, pathTo(name), subobjects, dynamicMapping);
        }

        void addCopyToCandidate(String leafFieldName) {
            eligibleCopyToDestinations.add(pathTo(leafFieldName));
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
