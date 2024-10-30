/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

import org.elasticsearch.entitlement.instrumentation.InstrumentationService;

module org.elasticsearch.entitlement.agent {
    requires java.instrument;
    requires org.elasticsearch.base; // for @SuppressForbidden

    exports org.elasticsearch.entitlement.instrumentation to org.elasticsearch.entitlement.agent.impl;

    uses InstrumentationService;
}
