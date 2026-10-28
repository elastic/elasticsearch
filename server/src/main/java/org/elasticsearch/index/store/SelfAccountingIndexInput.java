/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.IndexInput;

/**
 * An {@link IndexInput} that accounts the bytes it reads where it reads them, rather than being wrapped in a
 * {@link StoreMetricsIndexInput} that counts on every read call.
 * <p>
 * Such an input counts the bytes it read from the store, which is more than the bytes the caller consumed whenever a
 * read is only partly used, as when seeking around a file.
 */
public interface SelfAccountingIndexInput {

    /**
     * Accounts the bytes this input reads to {@code holder}, and passes it on to the inputs it hands out from
     * {@code clone()} and {@code slice()}.
     */
    void accountBytesReadTo(PluggableDirectoryMetricsHolder<StoreMetrics> holder);
}
