/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.rank.vectors {
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.server;
    requires org.apache.lucene.core;
    requires org.elasticsearch.xcontent;

    exports org.elasticsearch.xpack.rank.vectors;
    exports org.elasticsearch.xpack.rank.vectors.mapper;
    exports org.elasticsearch.xpack.rank.vectors.script;

    // whitelist resource access
    opens org.elasticsearch.xpack.rank.vectors.script to org.elasticsearch.painless.spi;

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.xpack.rank.vectors.script.RankVectorsPainlessExtension;
    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.rank.vectors.RankVectorsFeatures;

}
