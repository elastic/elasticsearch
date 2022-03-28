/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.logging.spi.ServerSupport;

module org.elasticsearch.logging {
    requires org.elasticsearch.cli;
    requires org.elasticsearch.core;
    requires org.elasticsearch.xcontent;
    requires log4j2.ecs.layout;
    requires ecs.logging.core;
    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;
    requires org.hamcrest;

    exports org.elasticsearch.logging;
    exports org.elasticsearch.logging.core;

    opens org.elasticsearch.logging.core to org.apache.logging.log4j.core;

    exports org.elasticsearch.logging.spi to org.elasticsearch.server;
    exports org.elasticsearch.logging.internal to org.elasticsearch.server;

    exports org.elasticsearch.logging.impl.testing;

    opens org.elasticsearch.logging.impl.testing to org.apache.logging.log4j.core;
    opens org.elasticsearch.logging.impl to org.apache.logging.log4j.core;

    uses ServerSupport;
}
