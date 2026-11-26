/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.util.List;

/**
 * A format's answer to whether its files hold collections that collection operators can read, and which. A format that
 * answers with collections also emits each one a read names in {@link FormatReadContext#collections()} as its
 * {@link ChildRelationColumns} carrier.
 */
public interface CollectionReads {
    /** For a format whose rows hold no collections. */
    CollectionReads NONE = object -> List.of();

    /** The collections in {@code object}'s schema. */
    List<CollectionSchema> collections(StorageObject object) throws IOException;
}
