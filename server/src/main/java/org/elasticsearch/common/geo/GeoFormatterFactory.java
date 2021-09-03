/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Geometry;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Output formatters for geo fields support extensions such as vector tiles.
 *
 * This class is an extensible version of a static GeometryFormatterFactory
 */
public class GeoFormatterFactory<T> {

    /**
     * Defines an extension point for geometry formatter
     * @param <T>
     */
    public interface FormatterFactory<T> {
        /**
         * Format name
         */
        String getName();

        /**
         * Generates a formatter builder that parses the formatter configuration and generates a formatter
         */
        Function<String, Function<List<T>, List<Object>>> getFormatterBuilder();
    }

    private final Map<String, Function<String, Function<List<T>, List<Object>>>> factories;

    /**
     * Creates an extensible geo formatter. The extension points can be added as a list of factories
     */
    public GeoFormatterFactory(List<FormatterFactory<T>> factories) {
        Map<String, Function<String, Function<List<T>, List<Object>>>> factoriesBuilder = new HashMap<>();
        for (FormatterFactory<T> factory : factories) {
            if(factoriesBuilder.put(factory.getName(), factory.getFormatterBuilder()) != null) {
                throw new IllegalArgumentException("More then one formatter factory with the name [" + factory.getName() +
                    "] was configured");
            }

        }
        this.factories = Collections.unmodifiableMap(factoriesBuilder);
    }

    /**
     * Returns a formatter by name
     *
     * The format can contain an optional parameters in parentheses such as "mvt(1/2/3)". Parameterless formats are getting resolved
     * using standard GeometryFormatterFactory and formats with parameters are getting resolved using factories specified during
     * construction.
     */
    public Function<List<T>, List<Object>> getFormatter(String format, Function<T, Geometry> toGeometry) {
        final int start = format.indexOf('(');
        if (start == -1)  {
            return GeometryFormatterFactory.getFormatter(format, toGeometry);
        }
        final String formatName = format.substring(0, start);
        Function<String, Function<List<T>, List<Object>>> factory = factories.get(formatName);
        if (factory == null) {
            throw new IllegalArgumentException("Invalid format: " + formatName);
        }
        final String param = format.substring(start + 1, format.length() - 1);
        return factory.apply(param);
    }
}
