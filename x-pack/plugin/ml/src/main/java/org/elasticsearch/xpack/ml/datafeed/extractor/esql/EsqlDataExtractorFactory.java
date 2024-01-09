/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
//
//import java.time.ZoneOffset;
//import java.util.List;
//import java.util.Locale;

public class EsqlDataExtractorFactory implements DataExtractorFactory {

    public static void create(DatafeedConfig datafeed, ActionListener<DataExtractorFactory> factoryHandler) {
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        return null;
    }
}
