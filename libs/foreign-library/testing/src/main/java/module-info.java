/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/** Test support for native libraries bound with {@code org.elasticsearch.foreign}. */
module org.elasticsearch.foreign.testing {
    requires org.elasticsearch.foreign;

    exports org.elasticsearch.foreign.testing;

    // `provides org.elasticsearch.foreign.LibraryProvider with ...PosixMemLibrary$Provider` is
    // injected into module-info.class by the build's augmentForeignModuleInfo task after
    // compilation; do NOT declare it here (the source cannot name the generated class).
}
