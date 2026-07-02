/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.Mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * This class handles any decision performed during data generation that changes the output.
 * For example: generating a random number, array of random size, mapping parameter.
 * <p>
 * Goals of this abstraction are:
 * <ul>
 * <li> to be able to easily add new types of decisions/generators </li>
 * <li> to decouple different types of decisions from each other, adding new data type should be an isolated additive change </li>
 * <li> to allow overriding only small specific subset of behavior (e.g. for testing purposes) </li>
 * </ul>
 */
public class DataSource {
    private final List<DataSourceHandler> handlers;
    private final IndexMode indexMode;

    public DataSource(Collection<DataSourceHandler> additionalHandlers) {
        this(additionalHandlers, IndexMode.STANDARD);
    }

    public DataSource(Collection<DataSourceHandler> additionalHandlers, IndexMode indexMode) {
        this.indexMode = indexMode;
        this.handlers = new ArrayList<>();

        this.handlers.addAll(additionalHandlers);

        this.handlers.add(new DefaultFieldDataGeneratorHandler());
        this.handlers.add(new DefaultPrimitiveTypesHandler());
        this.handlers.add(new DefaultWrappersHandler());
        this.handlers.add(new DefaultObjectGenerationHandler());
        this.handlers.add(new DefaultMappingParametersHandler());
    }

    public <T extends DataSourceResponse> T get(DataSourceRequest<T> request) {
        for (var handler : handlers) {
            var response = request.accept(handler);
            if (response != null) {
                return sanitizeForIndexMode(request, response);
            }
        }
        throw new IllegalStateException(
            "Request is not supported by data source. Request: "
                + request.toString()
                + "\n"
                + "Available handlers: "
                + handlers.stream().map(Object::getClass).map(Class::getName).toList().toString()
        );
    }

    /**
     * Some mapping parameters are only legal for specific index modes (e.g. {@code store} is never allowed, and the object form of
     * {@code doc_values} is only allowed, on strict-columnar indices). Rather than requiring every {@link DataSourceHandler} to be
     * aware of {@link IndexMode}, correct any handler's output here, in one place.
     */
    @SuppressWarnings("unchecked") // response was just matched to the same concrete type T is erased from
    private <T extends DataSourceResponse> T sanitizeForIndexMode(DataSourceRequest<T> request, T response) {
        boolean strictColumnar = indexMode.isStrictColumnar();
        if (response instanceof DataSourceResponse.LeafMappingParametersGenerator leaf) {
            return (T) new DataSourceResponse.LeafMappingParametersGenerator(
                () -> sanitizeLeafMapping(new HashMap<>(leaf.mappingGenerator().get()), strictColumnar)
            );
        }
        if (response instanceof DataSourceResponse.ObjectMappingParametersGenerator object) {
            boolean isRoot = ((DataSourceRequest.ObjectMappingParametersGenerator) request).isRoot();
            return (T) new DataSourceResponse.ObjectMappingParametersGenerator(
                () -> sanitizeObjectMapping(new HashMap<>(object.mappingGenerator().get()), isRoot, strictColumnar)
            );
        }
        return response;
    }

    private static Map<String, Object> sanitizeLeafMapping(Map<String, Object> mapping, boolean strictColumnar) {
        Object docValues = mapping.get("doc_values");
        if (strictColumnar) {
            // Every field on a strict-columnar index must be reconstructable from its own doc values.
            if (Boolean.FALSE.equals(docValues)) {
                mapping.put("doc_values", true);
            }
            // store, synthetic_source_keep and copy_to are not allowed on fields in strict-columnar mode.
            if (Boolean.TRUE.equals(mapping.get("store"))) {
                mapping.put("store", false);
            }
            mapping.remove(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM);
            mapping.remove("copy_to");
        } else if (docValues instanceof Map) {
            // The object form of doc_values (e.g. multi_value) is only valid in strict-columnar mode.
            mapping.put("doc_values", true);
        }
        return mapping;
    }

    private static Map<String, Object> sanitizeObjectMapping(Map<String, Object> mapping, boolean isRoot, boolean strictColumnar) {
        if (strictColumnar == false) {
            return mapping;
        }
        // subobjects and synthetic_source_keep are not allowed on objects in strict-columnar mode.
        mapping.remove("subobjects");
        mapping.remove(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM);
        // dynamic:runtime is not supported in strict-columnar mode.
        if ("runtime".equals(mapping.get("dynamic"))) {
            mapping.put("dynamic", randomFrom("true", "false", "strict"));
        }
        // enabled:false is not allowed on the root object in strict-columnar mode.
        if (isRoot && "false".equals(mapping.get("enabled"))) {
            mapping.put("enabled", "true");
        }
        return mapping;
    }
}
