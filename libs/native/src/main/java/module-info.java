/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.jdk.ModuleQualifiedExportsService;
import org.elasticsearch.nativeaccess.lib.NativeAccessModuleExportsService;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

module org.elasticsearch.nativeaccess {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;

    exports org.elasticsearch.nativeaccess to org.elasticsearch.server;
    exports org.elasticsearch.nativeaccess.lib to org.elasticsearch.nativeaccess.jna, org.elasticsearch.server;

    uses NativeLibraryProvider;
    provides ModuleQualifiedExportsService with NativeAccessModuleExportsService;
}
