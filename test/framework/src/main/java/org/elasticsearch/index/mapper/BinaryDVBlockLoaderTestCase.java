/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.index.IndexSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class BinaryDVBlockLoaderTestCase extends BlockLoaderTestCase {
    public record Params(boolean syntheticSource, MappedFieldType.FieldExtractPreference preference, boolean binaryDocValues) {
        public BlockLoaderTestCase.Params blTestCaseParams() {
            return new BlockLoaderTestCase.Params(syntheticSource, preference);
        }
    }

    protected final Params params;

    public BinaryDVBlockLoaderTestCase(String fieldType, Params params) {
        super(fieldType, params.blTestCaseParams());
        this.params = params;
    }

    public BinaryDVBlockLoaderTestCase(String fieldType, Collection<DataSourceHandler> customDataSourceHandlers, Params params) {
        super(fieldType, customDataSourceHandlers, params.blTestCaseParams());
        this.params = params;
    }

    @ParametersFactory(argumentFormatting = "params=%s")
    public static List<Object[]> args() {
        List<Object[]> args = new ArrayList<>();
        for (var preference : PREFERENCES) {
            for (boolean syntheticSource : new boolean[] { false, true }) {
                for (boolean useBinaryDocValues : new boolean[] { false, true }) {
                    args.add(new Object[] { new Params(syntheticSource, preference, useBinaryDocValues) });
                }
            }
        }
        return args;
    }

    @Override
    protected Settings.Builder getSettingsForParams() {
        var builder = Settings.builder();
        if (params.syntheticSource()) {
            builder.put("index.mapping.source.mode", "synthetic");
            if (params.binaryDocValues()) {
                builder.put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true);
            }
        }
        return builder;
    }
}
