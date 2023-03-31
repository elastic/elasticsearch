/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.preallocate {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires com.sun.jna;

    exports org.elasticsearch.preallocate to org.elasticsearch.blobcache;

    provides org.elasticsearch.jdk.ModuleQualifiedExportsService with org.elasticsearch.preallocate.PreallocateModuleExportsService;
}
