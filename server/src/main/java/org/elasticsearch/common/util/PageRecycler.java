/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.recycler.Recycler;

/// A recycler of fixed-size pages.
public interface PageRecycler {

    /// Obtains a byte page from the recycler.
    ///
    /// @param clear if `true`, the page will be zeroed out before being returned
    /// @return a recycled byte page
    Recycler.V<byte[]> bytePage(boolean clear);

    /// Obtains an object page from the recycler.
    /// Object pages are always cleared on release.
    ///
    /// @return a recycled object page
    Recycler.V<Object[]> objectPage();
}
