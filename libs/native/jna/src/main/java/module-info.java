/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.nativeaccess.jna.JnaNativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

module org.elasticsearch.nativeaccess.jna {
    requires org.elasticsearch.base;
    requires org.elasticsearch.nativeaccess;
    requires org.elasticsearch.logging;
    requires com.sun.jna;
    requires java.desktop;

    exports org.elasticsearch.nativeaccess.jna to com.sun.jna;

    provides NativeLibraryProvider with JnaNativeLibraryProvider;
}
