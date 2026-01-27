/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

import org.elasticsearch.jdk.ModuleQualifiedExportsService;
import org.elasticsearch.nativeaccess.exports.NativeAccessModuleExportsService;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

module org.elasticsearch.nativeaccess {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires java.management; // for access to heap size

    exports org.elasticsearch.nativeaccess
        to
            org.elasticsearch.server,
            org.elasticsearch.blobcache,
            org.elasticsearch.simdvec,
            org.elasticsearch.systemd;

    uses NativeLibraryProvider;

    provides ModuleQualifiedExportsService with NativeAccessModuleExportsService;

}
