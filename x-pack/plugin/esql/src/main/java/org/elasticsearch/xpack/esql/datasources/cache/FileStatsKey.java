/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

/**
 * The address of the STATISTICS measured over one file by one read.
 * <p>
 * It is its schema key plus the read, rather than a flat repetition of the schema key's components. Two
 * reasons, and the first is not cosmetic: the enrich path coerces every incoming per-column extremum to
 * the entry's RESOLVED column types before storing it, so that the whole-file fold never folds a Long
 * extremum against a Double one for the same column. Those types live on the schema record, and
 * {@link #schema()} is how a statistics address reaches it. Second, the path string is then stored once
 * per file rather than once per file per read: the schema key instance is shared, so a file read under
 * five configurations holds five statistics addresses over one copy of its path.
 * <p>
 * {@link ReadDecision} is what makes this an address and not a label. A statistic measures the rows one
 * read produced, so a harvest taken under one read configuration is stored under it and served to it -
 * where comparing the two at write time could only ever refuse, permanently, for any file read at a
 * shape other than its own.
 */
public record FileStatsKey(FileSchemaKey schema, ReadDecision decision) implements FileAddressed {

    @Override
    public String canonicalPath() {
        return schema.canonicalPath();
    }

    @Override
    public long lastModifiedEpochMillis() {
        return schema.lastModifiedEpochMillis();
    }

    public SourceScope scope() {
        return schema.scope();
    }
}
