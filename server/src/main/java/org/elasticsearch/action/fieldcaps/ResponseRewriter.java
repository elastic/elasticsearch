/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Applies field type filters to field caps responses that come from earlier versions of ES
 * that do not support filtering directly.
 */
final class ResponseRewriter {

    public static Map<String, IndexFieldCapabilities> rewriteOldResponses(
        TransportVersion version,
        Map<String, IndexFieldCapabilities> input,
        String[] filters,
        String[] allowedTypes
    ) {
        if (version.onOrAfter(TransportVersion.V_8_2_0)) {
            return input;   // nothing needs to be done
        }
        Function<IndexFieldCapabilities, IndexFieldCapabilities> transformer = buildTransformer(input, filters, allowedTypes);
        Map<String, IndexFieldCapabilities> rewritten = new HashMap<>();
        for (var entry : input.entrySet()) {
            IndexFieldCapabilities fc = transformer.apply(entry.getValue());
            if (fc != null) {
                rewritten.put(entry.getKey(), fc);
            }
        }
        return rewritten;
    }

    private static Function<IndexFieldCapabilities, IndexFieldCapabilities> buildTransformer(
        Map<String, IndexFieldCapabilities> input,
        String[] filters,
        String[] allowedTypes
    ) {
        Predicate<IndexFieldCapabilities> test = ifc -> true;
        Set<String> objects = null;
        Set<String> nestedObjects = null;
        if (allowedTypes.length > 0) {
            Set<String> at = Set.of(allowedTypes);
            test = test.and(ifc -> at.contains(ifc.getType()));
        }
        for (String filter : filters) {
            if ("-parent".equals(filter)) {
                test = test.and(fc -> fc.getType().equals("nested") == false && fc.getType().equals("object") == false);
            }
            if ("-metadata".equals(filter)) {
                test = test.and(fc -> fc.isMetadatafield() == false);
            }
            if ("+metadata".equals(filter)) {
                test = test.and(IndexFieldCapabilities::isMetadatafield);
            }
            if ("-nested".equals(filter)) {
                if (nestedObjects == null) {
                    nestedObjects = findTypes("nested", input);
                }
                Set<String> no = nestedObjects;
                test = test.and(fc -> isNestedField(fc.getName(), no) == false);
            }
            if ("-multifield".equals(filter)) {
                // immediate parent is not an object field
                if (objects == null) {
                    objects = findTypes("object", input);
                }
                Set<String> o = objects;
                test = test.and(fc -> isNotMultifield(fc.getName(), o));
            }
        }
        Predicate<IndexFieldCapabilities> finalTest = test;
        return fc -> {
            if (finalTest.test(fc) == false) {
                return null;
            }
            return fc;
        };
    }

    private static Set<String> findTypes(String type, Map<String, IndexFieldCapabilities> fieldCaps) {
        return fieldCaps.entrySet()
            .stream()
            .filter(entry -> type.equals(entry.getValue().getType()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    private static boolean isNestedField(String field, Set<String> nestedParents) {
        for (String parent : nestedParents) {
            if (field.startsWith(parent + ".") || field.equals(parent)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNotMultifield(String field, Set<String> objectFields) {
        int lastDotPos = field.lastIndexOf(".");
        if (lastDotPos == -1) {
            return true;
        }
        String parent = field.substring(0, lastDotPos);
        return objectFields.contains(parent);
    }

}
