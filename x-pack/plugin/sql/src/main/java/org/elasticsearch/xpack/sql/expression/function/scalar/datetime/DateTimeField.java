/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public interface DateTimeField {

    static <D extends DateTimeField> Map<String, D> initializeResolutionMap(D[] values) {
        Map<String, D> nameToPart = new HashMap<>();

        for (D datePart : values) {
            String lowerCaseName = datePart.name().toLowerCase(Locale.ROOT);

            nameToPart.put(lowerCaseName, datePart);
            for (String alias : datePart.aliases()) {
                nameToPart.put(alias, datePart);
            }
        }
        return Collections.unmodifiableMap(nameToPart);
    }

    static <D extends DateTimeField> List<String> initializeValidValues(D[] values) {
        return Arrays.stream(values).map(D::name).collect(Collectors.toList());
    }

    static <D extends DateTimeField> D resolveMatch(Map<String, D> resolutionMap, String possibleMatch) {
        return resolutionMap.get(possibleMatch.toLowerCase(Locale.ROOT));
    }

    static List<String> findSimilar(Iterable<String> similars, String match) {
        return StringUtils.findSimilar(match, similars);
    }

    String name();

    Iterable<String> aliases();
}
