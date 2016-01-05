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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.plugin.ingest.rest.IngestRestFilter;

public class IngestModule extends AbstractModule {

    private final boolean ingestEnabled;

    public IngestModule(boolean ingestEnabled) {
        this.ingestEnabled = ingestEnabled;
    }

    @Override
    protected void configure() {
        // Even if ingest isn't enabled we still need to make sure that rest requests with pipeline
        // param copy the pipeline into the context, so that in IngestDisabledActionFilter
        // index/bulk requests can be failed
        binder().bind(IngestRestFilter.class).asEagerSingleton();
        if (ingestEnabled) {
            binder().bind(IngestBootstrapper.class).asEagerSingleton();
        }
    }
}
