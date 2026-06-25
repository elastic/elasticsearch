/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.bytes.BytesArray;

import java.util.Locale;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;

public class ViewTestsUtils {
    public static String randomName() {
        return randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    public static View randomView(String name) {
        String query = "FROM " + randomAlphaOfLength(10);
        return new View(name, query);
    }

    /** A definer-rights view with a randomly populated definer identity. */
    public static View randomDefinerView(String name) {
        String query = "FROM " + randomAlphaOfLength(10);
        View.DefinerInfo definer = new View.DefinerInfo(
            randomAlphaOfLength(6),
            randomAlphaOfLength(6),
            randomBoolean() ? BytesArray.EMPTY : new BytesArray(randomAlphaOfLength(12).getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        return new View(name, query, View.RightsMode.DEFINER, definer);
    }
}
