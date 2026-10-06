/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

/**
 * A cache address that names ONE file, and the only two components of one that anything reads for their
 * value rather than comparing.
 * <p>
 * It exists because the reconcile cannot construct the address it wants to enrich. A harvest arriving
 * from a data node carries a path, an mtime and a fingerprint, but not the scope, so the write side
 * sweeps the whole store and destructures every key looking for a match. This interface is what that
 * sweep needs, and typing it has two consequences: the sweep can no longer see a set-level address at
 * all, which is what the discarded {@code #dataset-agg} marker was hand-enforcing; and the components
 * it does NOT expose - the scope and the read decision - are thereby proven to be compared and never
 * inspected, which is the licence for carrying them as folded lanes instead of the strings they came
 * from.
 * <p>
 * It is also the index key the sweep would need to stop being a sweep.
 */
public interface FileAddressed {

    /** The canonical path of the file this address names. */
    String canonicalPath();

    /** The file's last-modified time, in the address because a changed file is a different address. */
    long lastModifiedEpochMillis();
}
