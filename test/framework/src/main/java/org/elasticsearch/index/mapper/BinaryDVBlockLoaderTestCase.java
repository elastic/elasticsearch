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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class BinaryDVBlockLoaderTestCase extends BlockLoaderTestCase {
    public record Params(
        IndexMode indexMode,
        SourceFieldMapper.Mode sourceMode,
        MappedFieldType.FieldExtractPreference preference,
        boolean binaryDocValues
    ) {
        public BlockLoaderTestCase.Params blTestCaseParams() {
            return new BlockLoaderTestCase.Params(indexMode, sourceMode, preference);
        }

        public boolean syntheticSource() {
            return sourceMode == SourceFieldMapper.Mode.SYNTHETIC;
        }

        public boolean isColumnarStored() {
            return sourceMode == SourceFieldMapper.Mode.COLUMNAR_STORED;
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
        List<Object[]> parentArgs = BlockLoaderTestCase.args();
        List<Object[]> args = new ArrayList<>();
        for (Object[] parentArg : parentArgs) {
            for (boolean useBinaryDocValues : new boolean[] { false, true }) {
                BlockLoaderTestCase.Params parentParams = (BlockLoaderTestCase.Params) parentArg[0];
                args.add(
                    new Object[] {
                        new Params(parentParams.indexMode(), parentParams.sourceMode(), parentParams.preference(), useBinaryDocValues) }
                );
            }
        }
        return args;
    }

    @Override
    protected Settings.Builder getSettingsForParams() {
        var builder = super.getSettingsForParams();
        builder.put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), params.binaryDocValues());
        return builder;
    }
}
