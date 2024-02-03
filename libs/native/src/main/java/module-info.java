/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

module org.elasticsearch.nativeaccess {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    // to get
    requires java.management;

    exports org.elasticsearch.nativeaccess;
    // TODO: fix embedded loader to allow qualified exports to modules on modulepath
    exports org.elasticsearch.nativeaccess.lib; // to org.elasticsearch.nativeaccess.jna, org.elasticsearch.base;

    uses NativeLibraryProvider;
}
