/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * The X-Content module.
 */
module org.elasticsearch.xcontent {
    requires org.elasticsearch.base;

    exports org.elasticsearch.xcontent;
    exports org.elasticsearch.xcontent.cbor;
    exports org.elasticsearch.xcontent.json;
    exports org.elasticsearch.xcontent.smile;
    exports org.elasticsearch.xcontent.spi;
    exports org.elasticsearch.xcontent.support;
    exports org.elasticsearch.xcontent.support.filtering;
    exports org.elasticsearch.xcontent.yaml;

    uses org.elasticsearch.xcontent.ErrorOnUnknown;
    uses org.elasticsearch.xcontent.XContentBuilderExtension;
    uses org.elasticsearch.xcontent.spi.XContentProvider;
}
