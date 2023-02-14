/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.base {
    requires static jsr305;

    exports org.elasticsearch.core;
    exports org.elasticsearch.jdk;
    exports org.elasticsearch.core.internal.provider to org.elasticsearch.xcontent;
}
