/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.util.NamedSPILoader;

import java.util.Set;

/**
 * A {@link StoredFieldsFormat} that can be loaded via SPI and provides a name for identification.
 * This is required because {@link PerFieldStoredFieldsFormat} uses SPI to load stored field formats
 * when reading fields.
 */
public abstract class ESStoredFieldsFormat extends StoredFieldsFormat implements NamedSPILoader.NamedSPI {
    private static final class Holder {
        public static final NamedSPILoader<ESStoredFieldsFormat> LOADER = new NamedSPILoader<>(ESStoredFieldsFormat.class);

        private Holder() {}

        static NamedSPILoader<ESStoredFieldsFormat> getLoader() {
            if (LOADER == null) {
                throw new IllegalStateException(
                    "You tried to lookup a ESStoredFieldsFormat by name before all formats could be initialized."
                );
            }
            return LOADER;
        }
    }

    public static ESStoredFieldsFormat forName(String name) {
        return Holder.getLoader().lookup(name);
    }

    /**
     * Unique name that's used to retrieve this format when reading the index.
     */
    private final String name;

    protected ESStoredFieldsFormat(String name) {
        NamedSPILoader.checkServiceName(name);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the set of file fileExtensions that this stored fields format would write to disk.
     */
    protected abstract Set<String> getFileExtensions();
}
