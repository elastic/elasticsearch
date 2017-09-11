/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog.GetIndexResult;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public class FilteredCatalogTests extends ESTestCase {
    public void testGetTypeNoopCatalog() {
        Catalog orig = inMemoryCatalog("a", "b", "c");
        Catalog filtered = new FilteredCatalog(orig, delegateResult -> delegateResult);
        assertEquals(orig.getIndex("a"), filtered.getIndex("a"));
        assertEquals(orig.getIndex("b"), filtered.getIndex("b"));
        assertEquals(orig.getIndex("c"), filtered.getIndex("c"));
        assertEquals(orig.getIndex("missing"), filtered.getIndex("missing"));
    }

    public void testGetTypeFiltering() {
        Catalog orig = inMemoryCatalog("cat", "dog");
        Catalog filtered = new FilteredCatalog(orig, delegateResult -> {
            if (delegateResult.get().name().equals("cat")) {
                return delegateResult;
            }
            return GetIndexResult.notFound(delegateResult.get().name());
        });
        assertEquals(orig.getIndex("cat"), filtered.getIndex("cat"));
        assertEquals(GetIndexResult.notFound("dog"), filtered.getIndex("dog"));
    }

    public void testGetTypeModifying() {
        Catalog orig = inMemoryCatalog("cat", "dog");
        Catalog filtered = new FilteredCatalog(orig, delegateResult -> {
                EsIndex index = delegateResult.get();
                if (index.name().equals("cat")) {
                    return delegateResult;
                }
                return GetIndexResult.valid(new EsIndex("rat", index.mapping(), index.aliases(), index.settings()));
            });
        assertEquals(orig.getIndex("cat"), filtered.getIndex("cat"));
        assertEquals("rat", filtered.getIndex("dog").get().name());
    }

    public void testGetTypeFilterIgnoresMissing() {
        Catalog orig = inMemoryCatalog();
        Catalog filtered = new FilteredCatalog(orig, delegateResult -> {
            if (delegateResult.get().name().equals("cat")) {
                return delegateResult;
            }
            return GetIndexResult.notFound(delegateResult.get().name());
        });
        assertEquals(orig.getIndex("missing"), filtered.getIndex("missing"));
    }

    private Catalog inMemoryCatalog(String... indexNames) {
        List<EsIndex> indices = Arrays.stream(indexNames)
                .map(i -> new EsIndex(i, singletonMap("field", null), emptyList(), Settings.EMPTY))
                .collect(toList());
        return new InMemoryCatalog(indices);
    }
}
