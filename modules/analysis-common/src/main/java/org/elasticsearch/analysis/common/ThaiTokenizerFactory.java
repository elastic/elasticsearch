/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

/**
 * Factory for {@link ThaiTokenizer}. Indices created before {@link IndexVersions#UPGRADE_TO_LUCENE_10_6_0} get
 * {@link LegacyThaiTokenizer} so that their tokenization does not change after an upgrade.
 */
public class ThaiTokenizerFactory extends AbstractTokenizerFactory {

    private final boolean legacy;

    ThaiTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        this.legacy = indexSettings.getIndexVersionCreated().before(IndexVersions.UPGRADE_TO_LUCENE_10_6_0);
    }

    @Override
    public Tokenizer create() {
        return legacy ? new LegacyThaiTokenizer() : new ThaiTokenizer();
    }

    /**
     * Includes the side of the {@link IndexVersions#UPGRADE_TO_LUCENE_10_6_0} gate so that indices on either side never share
     * an analyzer built from this factory.
     */
    @Override
    public Object sharingKey() {
        return new Key(legacy);
    }

    private record Key(boolean legacy) {}
}
