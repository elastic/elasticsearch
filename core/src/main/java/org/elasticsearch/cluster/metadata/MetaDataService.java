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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.routing.DjbHashFunction;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.Semaphore;

/**
 */
public class MetaDataService extends AbstractComponent {

    private final Semaphore[] indexMdLocks;

    @Inject
    public MetaDataService(Settings settings) {
        super(settings);
        indexMdLocks = new Semaphore[500];
        for (int i = 0; i < indexMdLocks.length; i++) {
            indexMdLocks[i] = new Semaphore(1);
        }
    }

    public Semaphore indexMetaDataLock(String index) {
        return indexMdLocks[MathUtils.mod(DjbHashFunction.DJB_HASH(index), indexMdLocks.length)];
    }
}
