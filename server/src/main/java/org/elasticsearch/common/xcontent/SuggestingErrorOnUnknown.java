/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ErrorOnUnknown;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.stream.Collectors.toList;

public class SuggestingErrorOnUnknown implements ErrorOnUnknown {
    @Override
    public String errorMessage(String parserName, String unknownField, Iterable<String> candidates) {
        return String.format(Locale.ROOT, "[%s] unknown field [%s]%s", parserName, unknownField, suggest(unknownField, candidates));
    }

    @Override
    public int priority() {
        return 0;
    }

    /**
     * Builds suggestions for an unknown field, returning an empty string if there
     * aren't any suggestions or " did you mean " and then the list of suggestions.
     */
    public static String suggest(String unknownField, Iterable<String> candidates) {
        // TODO it'd be nice to combine this with BaseRestHandler's implementation.
        LevenshteinDistance ld = new LevenshteinDistance();
        final List<Tuple<Float, String>> scored = new ArrayList<>();
        for (String candidate : candidates) {
            float distance = ld.getDistance(unknownField, candidate);
            if (distance > 0.5f) {
                scored.add(new Tuple<>(distance, candidate));
            }
        }
        if (scored.isEmpty()) {
            return "";
        }
        CollectionUtil.timSort(scored, (a, b) -> {
            // sort by distance in reverse order, then parameter name for equal distances
            int compare = a.v1().compareTo(b.v1());
            if (compare != 0) {
                return -compare;
            }
            return a.v2().compareTo(b.v2());
        });
        List<String> keys = scored.stream().map(Tuple::v2).collect(toList());
        StringBuilder builder = new StringBuilder(" did you mean ");
        if (keys.size() == 1) {
            builder.append("[").append(keys.get(0)).append("]");
        } else {
            builder.append("any of ").append(keys.toString());
        }
        builder.append("?");
        return builder.toString();
    }
}
