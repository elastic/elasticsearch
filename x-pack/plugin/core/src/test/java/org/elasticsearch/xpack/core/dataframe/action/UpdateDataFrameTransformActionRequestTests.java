/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.action.UpdateDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.transforms.SyncConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.TimeSyncConfig;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigUpdateTests.randomDataFrameTransformConfigUpdate;

public class UpdateDataFrameTransformActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomDataFrameTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteables.add(new NamedWriteableRegistry.Entry(SyncConfig.class, DataFrameField.TIME_BASED_SYNC.getPreferredName(),
            TimeSyncConfig::new));
        return new NamedWriteableRegistry(namedWriteables);
    }
}
