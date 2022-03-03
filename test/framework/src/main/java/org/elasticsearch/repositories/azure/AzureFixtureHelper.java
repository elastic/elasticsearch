/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.Strings;

import java.util.Base64;

public class AzureFixtureHelper {
    private AzureFixtureHelper() {}

    public static boolean assertValidBlockId(String blockId) {
        assert Strings.hasText(blockId) : "blockId missing";
        try {
            final byte[] decode = Base64.getDecoder().decode(blockId);
            // all block IDs for a blob must be the same length and <64 bytes prior to decoding.
            // Elasticsearch generates them all to be 15 bytes long so we can just assert that:
            assert decode.length == 15 : "blockid [" + blockId + "] decodes to [" + decode.length + "] bytes";
        } catch (Exception e) {
            assert false : new AssertionError("blockid [" + blockId + "] is not in base64", e);
        }
        return true;
    }
}
