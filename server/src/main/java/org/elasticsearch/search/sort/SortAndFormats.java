/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.sort;

import org.apache.lucene.search.Sort;
import org.elasticsearch.search.DocValueFormat;

public final class SortAndFormats {

    public final Sort sort;
    public final DocValueFormat[] formats;

    public SortAndFormats(Sort sort, DocValueFormat[] formats) {
        if (sort.getSort().length != formats.length) {
            throw new IllegalArgumentException("Number of sort field mismatch: "
                    + sort.getSort().length + " != " + formats.length);
        }
        this.sort = sort;
        this.formats = formats;
    }

}
