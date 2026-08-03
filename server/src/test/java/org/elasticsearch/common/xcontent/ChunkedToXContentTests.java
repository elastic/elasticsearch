/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import static org.elasticsearch.common.xcontent.ChunkedToXContent.wrapAsToXContent;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.endObject;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.startObject;

public class ChunkedToXContentTests extends ESTestCase {

    public void testWrapAsToXContentProducesEqualInstances() {
        ChunkedToXContent object = params -> Iterators.concat(startObject(), endObject());

        ToXContent first = wrapAsToXContent(object);
        ToXContent second = wrapAsToXContent(object);

        assertNotSame(first, second);
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }
}
