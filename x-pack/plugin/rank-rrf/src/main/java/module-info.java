/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.xpack.rank.RankRRFFeatures;

module org.elasticsearch.rank.rrf {
    requires org.apache.lucene.core;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;

    exports org.elasticsearch.xpack.rank;
    exports org.elasticsearch.xpack.rank.rrf;
    exports org.elasticsearch.xpack.rank.linear;

    provides org.elasticsearch.features.FeatureSpecification with RankRRFFeatures;
}
