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

package org.elasticsearch.plugin.store.smb;

import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.store.smbmmapfs.SmbMmapFsIndexStore;
import org.elasticsearch.index.store.smbsimplefs.SmbSimpleFsIndexStore;
import org.elasticsearch.plugins.Plugin;

public class SMBStorePlugin extends Plugin {

    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addIndexStore("smb_mmap_fs", SmbMmapFsIndexStore::new);
        indexModule.addIndexStore("smb_simple_fs", SmbSimpleFsIndexStore::new);
    }
}
