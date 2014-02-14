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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.translog.fs.FsTranslog;

/**
 *
 */
public class TranslogModule extends AbstractModule {

    public static class TranslogSettings {
        public static final String TYPE = "index.translog.type";
    }

    private final Settings settings;

    public TranslogModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(Translog.class)
                .to(settings.getAsClass(TranslogSettings.TYPE, FsTranslog.class))
                .in(Scopes.SINGLETON);
        bind(TranslogService.class).asEagerSingleton();
    }
}
