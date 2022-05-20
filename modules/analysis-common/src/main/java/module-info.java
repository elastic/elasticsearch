/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.analysis.common {
    requires java.xml;

    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires org.apache.lucene.analysis.common;

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.analysis.common.AnalysisPainlessExtension;
}
