/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.util.Collection;
import java.util.List;

public abstract class BaseSearchableSnapshotsIntegTestCase extends ESIntegTestCase {
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SearchableSnapshots.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        if (randomBoolean()) {
            builder.put(
                CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(),
                rarely()
                    ? randomBoolean()
                        ? new ByteSizeValue(randomIntBetween(0, 10), ByteSizeUnit.KB)
                        : new ByteSizeValue(randomIntBetween(0, 1000), ByteSizeUnit.BYTES)
                    : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
            );
        }
        if (randomBoolean()) {
            builder.put(
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(),
                rarely()
                    ? new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB)
                    : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
            );
        }
        return builder.build();
    }
}
