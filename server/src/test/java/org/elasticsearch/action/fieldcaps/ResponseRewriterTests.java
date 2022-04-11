/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

public class ResponseRewriterTests extends ESTestCase {

    public void testExcludeMetadata() {
        Map<String, IndexFieldCapabilities> oldResponse = Map.of(
            "field",
            fieldCaps("field", "keyword", false),
            "_index",
            fieldCaps("_index", "_index", true)
        );

        Map<String, IndexFieldCapabilities> rewritten = ResponseRewriter.rewriteOldResponses(
            Version.V_8_0_0,
            oldResponse,
            new String[] { "-metadata" },
            Strings.EMPTY_ARRAY,
            f -> f.startsWith("_")
        );

        assertTrue(rewritten.containsKey("field"));
        assertFalse(rewritten.containsKey("_index"));
    }

    public void testIncludeOnlyMetadata() {
        Map<String, IndexFieldCapabilities> oldResponse = Map.of(
            "field",
            fieldCaps("field", "keyword", false),
            "_index",
            fieldCaps("_index", "_index", true)
        );

        Map<String, IndexFieldCapabilities> rewritten = ResponseRewriter.rewriteOldResponses(
            Version.V_8_0_0,
            oldResponse,
            new String[] { "+metadata" },
            Strings.EMPTY_ARRAY,
            f -> f.startsWith("_")
        );

        assertFalse(rewritten.containsKey("field"));
        assertTrue(rewritten.containsKey("_index"));
    }

    public void testExcludeNested() {
        Map<String, IndexFieldCapabilities> oldResponse = Map.of(
            "field",
            fieldCaps("field", "keyword", false),
            "parent",
            fieldCaps("parent", "nested", false),
            "parent.child",
            fieldCaps("parent.child", "keyword", false)
        );

        Map<String, IndexFieldCapabilities> rewritten = ResponseRewriter.rewriteOldResponses(
            Version.V_8_0_0,
            oldResponse,
            new String[] { "-nested" },
            Strings.EMPTY_ARRAY,
            f -> f.startsWith("_")
        );

        assertTrue(rewritten.containsKey("field"));
        assertFalse(rewritten.containsKey("parent.child"));
        assertFalse(rewritten.containsKey("parent"));
    }

    public void testExcludeMultifield() {
        Map<String, IndexFieldCapabilities> oldResponse = Map.of(
            "field",
            fieldCaps("field", "text", false),
            "field.keyword",
            fieldCaps("field.keyword", "keyword", false),
            "parent",
            fieldCaps("parent", "object", false),
            "parent.child",
            fieldCaps("parent.child", "keyword", false)
        );

        Map<String, IndexFieldCapabilities> rewritten = ResponseRewriter.rewriteOldResponses(
            Version.V_8_0_0,
            oldResponse,
            new String[] { "-multifield" },
            Strings.EMPTY_ARRAY,
            f -> f.startsWith("_")
        );

        assertTrue(rewritten.containsKey("field"));
        assertFalse(rewritten.containsKey("field.keyword"));
        assertTrue(rewritten.containsKey("parent.child"));
    }

    public void testExcludeParents() {
        Map<String, IndexFieldCapabilities> oldResponse = Map.of(
            "field",
            fieldCaps("field", "text", false),
            "parent",
            fieldCaps("parent", "object", false),
            "parent.child",
            fieldCaps("parent.child", "keyword", false)
        );

        Map<String, IndexFieldCapabilities> rewritten = ResponseRewriter.rewriteOldResponses(
            Version.V_8_0_0,
            oldResponse,
            new String[] { "-parent" },
            Strings.EMPTY_ARRAY,
            f -> f.startsWith("_")
        );

        assertTrue(rewritten.containsKey("field"));
        assertFalse(rewritten.containsKey("parent"));
        assertTrue(rewritten.containsKey("parent.child"));
    }

    public void testAllowedTypes() {
        Map<String, IndexFieldCapabilities> oldResponse = Map.of(
            "text",
            fieldCaps("text", "text", false),
            "long",
            fieldCaps("long", "long", false),
            "keyword",
            fieldCaps("keyword", "keyword", false)
        );

        Map<String, IndexFieldCapabilities> rewritten = ResponseRewriter.rewriteOldResponses(
            Version.V_8_0_0,
            oldResponse,
            Strings.EMPTY_ARRAY,
            new String[] { "text", "keyword" },
            f -> f.startsWith("_")
        );

        assertTrue(rewritten.containsKey("text"));
        assertTrue(rewritten.containsKey("keyword"));
        assertFalse(rewritten.containsKey("long"));
    }

    private static IndexFieldCapabilities fieldCaps(String name, String type, boolean isMetadata) {
        return new IndexFieldCapabilities(name, type, isMetadata, true, true, false, null, Collections.emptyMap());
    }

}
