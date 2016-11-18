/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.elasticsearch.xpack.prelert.job.results.ReservedFieldNames;

/**
 * Interprets field names containing dots as nested JSON structures.
 * This matches what Elasticsearch does.
 */
class ElasticsearchDotNotationReverser
{
    private static final char DOT = '.';
    private static final Pattern DOT_PATTERN = Pattern.compile("\\.");

    private final Map<String, Object> resultsMap;

    public ElasticsearchDotNotationReverser()
    {
        resultsMap = new TreeMap<>();
    }

    // TODO - could handle values of all types Elasticsearch does, e.g. date,
    // long, int, double, etc.  However, at the moment field values in our
    // results are only strings, so it's not "minimum viable product" right
    // now.  Hence this method only takes fieldValue as a String and there are
    // no overloads.
    /**
     * Given a field name and value, convert it to a map representation of the
     * (potentially nested) JSON structure Elasticsearch would use to store it.
     * For example:
     * <code>foo = x</code> goes to <code>{ "foo" : "x" }</code> and
     * <code>foo.bar = y</code> goes to <code>{ "foo" : { "bar" : "y" } }</code>
     */
    @SuppressWarnings("unchecked")
    public void add(String fieldName, String fieldValue)
    {
        if (fieldName == null || fieldValue == null)
        {
            return;
        }

        // Minimise processing in the simple case of no dots in the field name.
        if (fieldName.indexOf(DOT) == -1)
        {
            if (ReservedFieldNames.RESERVED_FIELD_NAMES.contains(fieldName))
            {
                return;
            }
            resultsMap.put(fieldName, fieldValue);
            return;
        }

        String[] segments = DOT_PATTERN.split(fieldName);

        // If any segment created by the split is a reserved word then ignore
        // the whole field.
        for (String segment : segments)
        {
            if (ReservedFieldNames.RESERVED_FIELD_NAMES.contains(segment))
            {
                return;
            }
        }

        Map<String, Object> layerMap = resultsMap;
        for (int i = 0; i < segments.length; ++i)
        {
            String segment = segments[i];
            if (i == segments.length - 1)
            {
                layerMap.put(segment, fieldValue);
            }
            else
            {
                Object existingLayerValue = layerMap.get(segment);
                if (existingLayerValue == null)
                {
                    Map<String, Object> nextLayerMap = new TreeMap<>();
                    layerMap.put(segment, nextLayerMap);
                    layerMap = nextLayerMap;
                }
                else
                {
                    if (existingLayerValue instanceof Map)
                    {
                        layerMap = (Map<String, Object>)existingLayerValue;
                    }
                    else
                    {
                        // This implies an inconsistency - different additions
                        // imply the same path leads to both an object and a
                        // value.  For example:
                        // foo.bar = x
                        // foo.bar.baz = y
                        return;
                    }
                }
            }
        }
    }

    public Map<String, Object> getResultsMap()
    {
        return resultsMap;
    }

    /**
     * Mappings for a given hierarchical structure are more complex than the
     * basic results.
     */
    public Map<String, Object> getMappingsMap()
    {
        Map<String, Object> mappingsMap = new TreeMap<>();
        recurseMappingsLevel(resultsMap, mappingsMap);
        return mappingsMap;
    }

    @SuppressWarnings("unchecked")
    private void recurseMappingsLevel(Map<String, Object> resultsMap,
            Map<String, Object> mappingsMap)
    {
        for (Map.Entry<String, Object> entry : resultsMap.entrySet())
        {
            Map<String, Object> typeMap = new TreeMap<>();

            String name = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map)
            {
                Map<String, Object> propertiesMap = new TreeMap<>();
                recurseMappingsLevel((Map<String, Object>)value, propertiesMap);

                typeMap.put(ElasticsearchMappings.TYPE,
                        ElasticsearchMappings.OBJECT);
                typeMap.put(ElasticsearchMappings.PROPERTIES, propertiesMap);
                mappingsMap.put(name, typeMap);
            }
            else
            {
                String fieldType = value.getClass().getSimpleName().toLowerCase(Locale.ROOT);
                if ("string".equals(fieldType)) {
                    fieldType = "keyword";
                }
                typeMap.put(ElasticsearchMappings.TYPE,
                        // Even though the add() method currently only supports
                        // strings, this way of getting the type would work for
                        // many Elasticsearch types, e.g. date, int, long,
                        // double and boolean
                        fieldType);
                mappingsMap.put(name, typeMap);
            }
        }
    }
}
