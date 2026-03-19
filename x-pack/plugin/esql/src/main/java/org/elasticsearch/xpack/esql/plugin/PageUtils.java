/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for splitting and slicing {@link Page} lists into fixed-size output pages.
 */
public final class PageUtils {

    private PageUtils() {}

    /**
     * Extracts a window of rows [fromRow, fromRow + maxRows) from the given pages,
     * returning new Page objects that share the underlying block data via shallow copies.
     */
    public static List<Page> slicePages(List<Page> pages, int fromRow, int maxRows) {
        List<Page> result = new ArrayList<>();
        int remaining = maxRows;
        int skipped = 0;

        for (Page page : pages) {
            int pageRows = page.getPositionCount();
            if (skipped + pageRows <= fromRow) {
                skipped += pageRows;
                continue;
            }

            int startInPage = Math.max(0, fromRow - skipped);
            int endInPage = Math.min(pageRows, startInPage + remaining);
            int take = endInPage - startInPage;

            if (take > 0) {
                int[] positions = new int[take];
                for (int i = 0; i < take; i++) {
                    positions[i] = startInPage + i;
                }
                result.add(page.shallowCopy().filter(false, positions));
                remaining -= take;
                if (remaining <= 0) {
                    break;
                }
            }
            skipped += pageRows;
        }
        return result;
    }
}
