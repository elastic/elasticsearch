/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import java.util.stream.Stream;

public class StreamUtils {

    private StreamUtils() {
        // no instances
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Stream<T> concat(Stream<T>... streams) {
        if (streams.length == 0) {
            return Stream.of();
        }
        var stream = streams[streams.length - 1];
        for (int i = streams.length - 2; i >= 0; i--) {
            stream = Stream.concat(streams[i], stream);
        }
        return stream;
    }
}
