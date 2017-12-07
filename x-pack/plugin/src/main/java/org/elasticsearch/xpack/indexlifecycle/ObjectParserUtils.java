/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.common.io.stream.NamedWriteable;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * NOCOMMIT: these utility methods should be brought into the core API for parsing namedWriteables
 */
public class ObjectParserUtils {
    public static <V extends NamedWriteable> SortedMap<String, V>  convertListToMapValues(Function<V, String> keyFunction,
                                                                                          List<V> list) {
        SortedMap<String, V> map = new TreeMap<>();
        for (V namedWriteable : list) {
            map.put(keyFunction.apply(namedWriteable), namedWriteable);
        }
        return map;
    }

}
