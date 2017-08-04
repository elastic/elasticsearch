/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.catalog.FilteredCatalog.Filter;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public class FilteredCatalogTests extends ESTestCase {
    public void testGetTypeNoopCatalog() {
        Catalog orig = inMemoryCatalog("a", "b", "c");
        Catalog filtered = new FilteredCatalog(orig, new Filter() {
            @Override
            public EsIndex filterIndex(EsIndex index) {
                return index;
            }
        });
        assertSame(orig.getIndex("a"), filtered.getIndex("a"));
        assertSame(orig.getIndex("b"), filtered.getIndex("b"));
        assertSame(orig.getIndex("c"), filtered.getIndex("c"));
        assertSame(orig.getIndex("missing"), filtered.getIndex("missing"));
    }

    public void testGetTypeFiltering() {
        Catalog orig = inMemoryCatalog("cat", "dog");
        Catalog filtered = new FilteredCatalog(orig, new Filter() {
            @Override
            public EsIndex filterIndex(EsIndex index) {
                return index.name().equals("cat") ? index : null;
            }
        });
        assertSame(orig.getIndex("cat"), filtered.getIndex("cat"));
        assertNull(filtered.getIndex("dog"));
    }

    public void testGetTypeModifying() {
        Catalog orig = inMemoryCatalog("cat", "dog");
        Catalog filtered = new FilteredCatalog(orig, new Filter() {
            @Override
            public EsIndex filterIndex(EsIndex index) {
                if (index.name().equals("cat")) {
                    return index;
                }
                return new EsIndex("rat", index.mapping(), index.aliases(), index.settings());
            }
        });
        assertSame(orig.getIndex("cat"), filtered.getIndex("cat"));
        assertEquals("rat", filtered.getIndex("dog").name());
    }

    public void testGetTypeFilterIgnoresNull() {
        Catalog filtered = new FilteredCatalog(inMemoryCatalog(), new Filter() {
            @Override
            public EsIndex filterIndex(EsIndex index) {
                return index.name().equals("cat") ? index : null;
            }
        });
        assertNull(filtered.getIndex("missing"));
    }

    private Catalog inMemoryCatalog(String... indexNames) {
        List<EsIndex> indices = Arrays.stream(indexNames)
                .map(i -> new EsIndex(i, singletonMap("field", null), emptyList(), Settings.EMPTY))
                .collect(toList());
        return new InMemoryCatalog(indices);
    }
}
