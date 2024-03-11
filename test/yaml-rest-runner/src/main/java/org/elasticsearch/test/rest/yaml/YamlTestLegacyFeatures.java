/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;

/**
 * This class groups historical features that have been removed from the production codebase, but are still used by YAML test
 * to support BwC. Rather than leaving them in the main src we group them here, so it's clear they are not used in production code anymore.
 */
public class YamlTestLegacyFeatures implements FeatureSpecification {

    private static final NodeFeature CAT_ALIASES_SHOW_WRITE_INDEX = new NodeFeature("cat_aliases_show_write_index");

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.ofEntries(Map.entry(CAT_ALIASES_SHOW_WRITE_INDEX, Version.V_7_4_0));
    }
}
