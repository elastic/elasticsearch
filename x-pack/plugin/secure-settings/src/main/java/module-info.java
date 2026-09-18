/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;
import org.elasticsearch.xpack.stateless.settings.secure.ReservedSecureSettingsHandlerProvider;

module org.elasticsearch.settings.secure {
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcore;

    provides ReservedStateHandlerProvider with ReservedSecureSettingsHandlerProvider;
}
