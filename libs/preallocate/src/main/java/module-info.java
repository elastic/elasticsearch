/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.preallocate {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires com.sun.jna;

    exports org.elasticsearch.preallocate to org.elasticsearch.blobcache, com.sun.jna;

    provides org.elasticsearch.jdk.ModuleQualifiedExportsService with org.elasticsearch.preallocate.PreallocateModuleExportsService;
}
