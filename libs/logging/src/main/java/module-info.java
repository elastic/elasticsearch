/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.logging.spi.AppenderSupport;
import org.elasticsearch.logging.spi.LogLevelSupport;
import org.elasticsearch.logging.spi.LogManagerFactory;
import org.elasticsearch.logging.spi.LoggingBootstrapSupport;
import org.elasticsearch.logging.spi.MessageFactory;
import org.elasticsearch.logging.spi.ServerSupport;
import org.elasticsearch.logging.spi.StringBuildersSupport;

module org.elasticsearch.logging {
    requires org.elasticsearch.cli;
    requires org.elasticsearch.core;
    requires org.elasticsearch.xcontent;
    requires org.hamcrest;

    exports org.elasticsearch.logging;

    exports org.elasticsearch.logging.core ;//to org.elasticsearch.x_pack.deprecation, org.elasticsearch.logging.impl;
    opens org.elasticsearch.logging.core to org.apache.logging.log4j.core;
    exports org.elasticsearch.logging.bootstrap to org.elasticsearch.server;
//    exports org.elasticsearch.logging.impl.testing;

//    opens org.elasticsearch.logging.impl.testing to org.apache.logging.log4j.core;
    exports org.elasticsearch.logging.spi;

    uses ServerSupport;
    uses MessageFactory;
    uses LogLevelSupport;
    uses LoggingBootstrapSupport;
    uses AppenderSupport;
    uses LogManagerFactory;
    uses StringBuildersSupport;

}
