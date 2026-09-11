/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.zstd {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires transitive org.elasticsearch.foreign;
    requires org.elasticsearch.foreign.adapter;

    exports org.elasticsearch.zstd
        to
            org.elasticsearch.server,
            org.elasticsearch.columnar,
            org.elasticsearch.xpack.esql.datasource.compress;

    // `provides org.elasticsearch.foreign.LibraryProvider with ...ZstdLibrary$Provider` is
    // injected into module-info.class by the build's augmentForeignModuleInfo task after
    // compilation; do NOT declare it here (the source cannot name the generated class).
}
