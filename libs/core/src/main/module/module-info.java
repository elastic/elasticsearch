/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.core {
    requires static /*transitive*/ jsr305; // ####: does this need to be transitive

    exports org.elasticsearch.core;
    exports org.elasticsearch.jdk;

    // for now..
    exports org.elasticsearch.core.internal.io; // to org.elasticsearch.xcontent,org.elasticsearch.cli,org.elasticsearch.server;
    exports org.elasticsearch.core.internal.net; // to org.elasticsearch.xcontent;
}
