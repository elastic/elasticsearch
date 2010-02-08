/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.deletionpolicy;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.elasticsearch.index.shard.IndexShardLifecycle;
import org.elasticsearch.util.settings.Settings;

import static org.elasticsearch.index.deletionpolicy.DeletionPolicyModule.DeletionPolicySettings.*;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
public class DeletionPolicyModule extends AbstractModule {

    public static class DeletionPolicySettings {
        public static final String TYPE = "index.deletionpolicy.type";
    }

    private final Settings settings;

    public DeletionPolicyModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        bind(IndexDeletionPolicy.class)
                .annotatedWith(Names.named("actual"))
                .to(settings.getAsClass(TYPE, KeepOnlyLastDeletionPolicy.class))
                .asEagerSingleton();

        bind(SnapshotDeletionPolicy.class)
                .asEagerSingleton();
    }
}
