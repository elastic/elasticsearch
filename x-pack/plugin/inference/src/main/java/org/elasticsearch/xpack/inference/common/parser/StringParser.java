/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.Strings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.pathToKey;

public final class StringParser {

    @SuppressWarnings("unchecked")
    public static List<String> extractStringList(Map<String, Object> map, String key, String root) {
        var list = ObjectParserUtils.removeAsType(map, key, root, List.class);
        if (list == null) {
            return List.of();
        }

        for (int i = 0; i < list.size(); i++) {
            var item = list.get(i);
            if (item instanceof String == false) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Expected all items in list for field [%s] to be of type String but item [%s] at index [%d] is of type [%s]",
                        pathToKey(root, key),
                        item,
                        i,
                        item.getClass().getSimpleName()
                    )
                );
            }
        }

        return (List<String>) list;
    }

    private StringParser() {}
}
