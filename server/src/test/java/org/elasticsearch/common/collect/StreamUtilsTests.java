/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.Stream;

public class StreamUtilsTests extends ESTestCase {

    public void testConcat() {
        assertEquals(List.of(), StreamUtils.concat().toList());
        assertEquals(List.of(1, 2, 3), StreamUtils.concat(Stream.of(1, 2, 3)).toList());
        assertEquals(List.of(1, 2, 3, 4), StreamUtils.concat(Stream.of(1), Stream.of(2), Stream.of(3, 4)).toList());
    }

}
